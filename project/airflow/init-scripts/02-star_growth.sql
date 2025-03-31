CREATE EXTERNAL TABLE `dz-final-project.gharchive.star_growth` (
    id INT64,                     -- 唯一識別
    repo_name STRING NOT NULL,     -- repo 名稱
    star_growth INT64 NOT NULL,    -- Star 成長數量
    analysis_date DATE NOT NULL,   -- 分析日期（每日統計）
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- 記錄創建時間
)
OPTIONS (
    uris = ['gs://dz-data-lake/gharchive/*.json.gz'],  -- 指定 GCS 檔案路徑
    format = 'JSON',  -- 明確指定格式
    compression = 'GZIP'  -- 指定 GZIP 壓縮格式
);

