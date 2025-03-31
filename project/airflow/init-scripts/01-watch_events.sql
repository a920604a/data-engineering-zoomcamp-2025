CREATE EXTERNAL TABLE `dz-final-project.gharchive.watch_events_external_table` (
    id STRING,         -- GitHub 事件的唯一 ID
    repo_name STRING,
    public BOOL,       -- 移除 NOT NULL，避免 JSON 資料缺失時出錯
    created_at TIMESTAMP
)
OPTIONS (
    uris = ['gs://dz-data-lake/gharchive/*.json.gz'],  -- 指定 GCS 檔案路徑
    format = 'JSON',  -- 明確指定格式
    compression = 'GZIP'  -- 指定 GZIP 壓縮格式
);
