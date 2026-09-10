import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def get_connection():
    """Open a Snowflake connection using credentials from .env"""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

if __name__ == "__main__":
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_WAREHOUSE()")
        version, user, wh = cur.fetchone()
        print(f"Connected - Snowflake {version} | user: {user} | warehouse: {wh}")
    finally:
        conn.close()