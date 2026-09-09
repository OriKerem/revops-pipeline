# Data Profiling Notes

## sales_pipeline — 8,800 rows
- deal_stage: Won 4,238 | Lost 2,473 | Engaging 1,589 | Prospecting 500
- Win rate (closed deals only): ~63%
- close_value is NULL for open deals (Engaging, Prospecting)
- close_value = 0 for Lost deals, not NULL — filter to Won for avg deal size
- Date range: 2016-10-20 to 2017-12-31
- No duplicate opportunity_id
- No orphan foreign keys — all accounts and sales_agents resolve