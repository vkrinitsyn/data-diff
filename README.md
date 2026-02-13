# data-diff
PostgreSQL DB data diff compare

Identify schema diff:
use schema_guard_tokio crate fn load_info_schema() from ..\schema_guard\ to get table schema, and compare (use diff_doc from ..\diff-doc-rs) with source and target,
if mismatch but target has more columns than source OR table is in exclusion, than proceed with diff, otherwise,
skip diff and output a message of schema mismatch and request to add table to exclusion or define a timestamp column for diff.
remove ignore_missing_target

Identify chunk strategy:

1. figure out a timestamp column name: update* if not exists then create* timestamp,
   -or use a DiffConfig defined column name for common a timestamp column name.

2. table size:
    - if more than 1M row, than if no timestamp column and table not in exclude list, than suggest to:
        - define a timestamp column name (see above)
        - or add table to exclude list
    - if more than 1M row and timestamp column defined, than:
        - use md5 hash of chunk of tables split by DiffConfig defined max % chunk size (default 10%) to identify chunk of data,
        - found as max(timestamp column) - min (timstamp column) in days / 10, than use this chunk size to split table into chunks, and compare each chunk separately.
          sample SQL for timestamp chunks: SELECT md5(t.*::text) FROM <table_name> where timestamp > $1 and timestamp <= $2;
    - if less than 1M row, than compare table as split by id chunks
      sample SQL for id chunks: SELECT md5(t.*::text) FROM <table_name> where id in ($1) ;
    - if less than 100K row, than compare table as split by id chunks
      sample SQL for id: SELECT md5(t.*::text) FROM <table_name> where id in ($1) ;

3: ETC(completion) estimation:
- check each tables total size, define timestamp column, try to md5 a 1/10 of chunk but more than 100K rows, and estimate time for each chunk, than sum up for total ETC estimation.
- if takes more than 3 hour, than exist with a message of ETC and request a special flag for force a task run.
- progress flags is on by default, but if --no-progress flag is set, than no progress output.
