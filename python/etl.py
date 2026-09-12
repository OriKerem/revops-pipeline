"""
ETL: RAW -> ANALYTICS star schema.
Reads raw CRM tables from Snowflake, builds dimension and fact tables,
enriches with live FX rates, writes back to the ANALYTICS schema.
"""

import logging
import pandas as pd
import requests
from snowflake.connector.pandas_tools import write_pandas
from connection import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl")


###Extract

RAW_TABLES = ["sales_pipeline", "accounts", "products", "sales_teams"]


def read_table(conn, table):
    """Read one RAW table into a DataFrame with lowercase column names."""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM revops.raw.{table}")
        df = cur.fetch_pandas_all()
    finally:
        cur.close()
    df.columns = [c.lower() for c in df.columns]
    return df


def extract(conn):
    """Pull all raw tables into memory."""
    data = {}
    for table in RAW_TABLES:
        df = read_table(conn, table)
        log.info("Extracted %s: %s rows", table, len(df))
        data[table] = df
    return data


## API curency rate

FX_FALLBACK = {"ILS": 3.70, "EUR": 0.92}


def fetch_fx_rates(base="USD", symbols=("ILS", "EUR")):
    """Fetch live FX rates. Falls back to static rates if the API is unreachable."""
    url = f"https://api.frankfurter.app/latest?base={base}&symbols={','.join(symbols)}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        rates = resp.json()["rates"]
        log.info("Fetched FX rates from API: %s", rates)
        return rates
    except Exception as exc:
        log.warning("FX API failed (%s) - using fallback rates", exc)
        return FX_FALLBACK


## Transform

STAGE_PROBABILITY = {
    "Prospecting": 0.10,
    "Engaging": 0.40,
    "Won": 1.00,
    "Lost": 0.00,
}


def build_dimensions(data):
    """Build the three dimension tables."""
    dim_account = data["accounts"].copy()
    dim_account["company_size"] = pd.cut(
        dim_account["employees"],
        bins=[0, 500, 2500, 10000, float("inf")],
        labels=["Small", "Mid", "Large", "Enterprise"],
    ).astype(str)

    dim_agent = data["sales_teams"].copy()
    dim_product = data["products"].copy()

    log.info(
        "Dimensions: %s accounts, %s agents, %s products",
        len(dim_account), len(dim_agent), len(dim_product),
    )
    return dim_account, dim_agent, dim_product


def build_fact(data, rates):
    """Build the opportunity fact table with derived measures."""
    df = data["sales_pipeline"].copy()

    # bring in list price so open deals can be valued
    df = df.merge(
        data["products"][["product", "sales_price"]],
        on="product",
        how="left",
    )

    df["engage_date"] = pd.to_datetime(df["engage_date"])
    df["close_date"] = pd.to_datetime(df["close_date"])

    df["is_closed"] = df["deal_stage"].isin(["Won", "Lost"]).astype(int)
    df["is_won"] = (df["deal_stage"] == "Won").astype(int)

    # Lost deals carry close_value = 0, so revenue must be Won-only
    df["revenue_usd"] = df["close_value"].where(df["deal_stage"] == "Won")

    # probability-weighted value of the open pipeline
    df["win_probability"] = df["deal_stage"].map(STAGE_PROBABILITY)
    df["expected_value_usd"] = df["sales_price"] * df["win_probability"]

    df["sales_cycle_days"] = (df["close_date"] - df["engage_date"]).dt.days
    df["close_quarter"] = df["close_date"].dt.to_period("Q").astype(str)
    df["close_month"] = df["close_date"].dt.to_period("M").astype(str)

    df["deal_size_band"] = pd.cut(
        df["revenue_usd"],
        bins=[0, 1000, 5000, 20000, float("inf")],
        labels=["<1K", "1K-5K", "5K-20K", "20K+"],
    ).astype(str)

    for currency, rate in rates.items():
        df[f"revenue_{currency.lower()}"] = df["revenue_usd"] * rate

        #df["revenue_eur"] = df["revenue_usd"] * 0.85822
        #df["revenue_ils"] = df["revenue_usd"] * 3.0192

    #log.info("Fact table: %s rows, %s measures", len(df), df.shape[1])
    rows, cols = df.shape
    log.info("Fact table: %s rows, %s measures", rows, cols)
    return df

##Load - Taking a DataFrame from memory and stores it as a table in Snowflake
#write_pandas is the Snowflake function that uploads an entire DataFrame at once.
# It doesn't insert row by row - behind the scenes it writes a temporary file and runs COPY INTO.
# That's why 8,800 rows loaded in two seconds.

def load_table(conn, df, table_name):
    """Write a DataFrame to the ANALYTICS schema, replacing any existing table."""
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    success, _, nrows, _ = write_pandas(
        conn,
        df,
        table_name.upper(),
        database="REVOPS",
        schema="ANALYTICS",
        auto_create_table=True,
        overwrite=True,
    )
    if not success:
        raise RuntimeError(f"Failed to load {table_name}")
    log.info("Loaded %s: %s rows", table_name.upper(), nrows)

## Validate + main

def validate(conn):
    """Data quality checks derived from the profiling notes."""
    checks = [
        ("fact has rows",
         "SELECT COUNT(*) FROM revops.analytics.fact_opportunity",
         lambda v: v > 0),
        ("no duplicate opportunity ids",
         "SELECT COUNT(*) - COUNT(DISTINCT opportunity_id) "
         "FROM revops.analytics.fact_opportunity",
         lambda v: v == 0),
        ("all agents resolve to a dimension",
         "SELECT COUNT(*) FROM revops.analytics.fact_opportunity f "
         "LEFT JOIN revops.analytics.dim_agent a ON f.sales_agent = a.sales_agent "
         "WHERE a.sales_agent IS NULL",
         lambda v: v == 0),
        ("won revenue is positive",
         "SELECT COALESCE(SUM(revenue_usd), 0) FROM revops.analytics.fact_opportunity "
         "WHERE is_won = 1",
         lambda v: v > 0),
    ]

    cur = conn.cursor()
    failures = 0
    try:
        for name, sql, predicate in checks:
            value = cur.execute(sql).fetchone()[0]
            if predicate(value):
                log.info("PASS  %-38s -> %s", name, value)
            else:
                log.error("FAIL  %-38s -> %s", name, value)
                failures += 1
    finally:
        cur.close()

    if failures:
        raise RuntimeError(f"{failures} data quality check(s) failed")
    log.info("All data quality checks passed")


def main():
    conn = get_connection()
    try:
        data = extract(conn)
        rates = fetch_fx_rates()

        dim_account, dim_agent, dim_product = build_dimensions(data)
        fact = build_fact(data, rates)

        load_table(conn, dim_account, "dim_account")
        load_table(conn, dim_agent, "dim_agent")
        load_table(conn, dim_product, "dim_product")
        load_table(conn, fact, "fact_opportunity")

        validate(conn)
        log.info("ETL completed successfully")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

