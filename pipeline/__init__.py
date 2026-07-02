# Urban Mobility Signal Pipeline. Marks `pipeline` as an importable package so the
# BigQuery/GenAI stages can share pipeline.bq_common regardless of the cwd
# they're launched from (each script inserts the repo root onto sys.path).
