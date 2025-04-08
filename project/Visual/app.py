import streamlit as st
import pandas as pd
from google.cloud import bigquery

# 初始化 BigQuery 客戶端
client = bigquery.Client()

# 設定資料庫連接
BQ_PROJECT = "dz-final-project"
BQ_DATASET = "gharchive"
BQ_TABLE = "github_archive"

def get_push_events(date, hour):
    query = f"""
        SELECT repo.name AS repo, push_count
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        WHERE FORMAT_TIMESTAMP('%Y-%m-%d', date_hour) = '{date}'
        AND EXTRACT(HOUR FROM date_hour) = {hour}
        ORDER BY push_count DESC
        LIMIT 10
    """
    df = client.query(query).to_dataframe()
    return df

# 用戶界面
st.title("📈 GitHub PushEvent 熱門 Repo")

# 選擇日期與小時
selected_date = st.date_input("選擇日期")
selected_hour = st.slider("選擇小時", 0, 23)

# 顯示資料
df = get_push_events(selected_date, selected_hour)
if df.empty:
    st.write("沒有資料")
else:
    st.bar_chart(df.set_index("repo"))
    st.table(df)
