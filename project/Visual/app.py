import streamlit as st
import pandas as pd
from google.cloud import bigquery

# 初始化 BigQuery 客戶端
client = bigquery.Client()

# 設定資料庫連接
BQ_PROJECT = "dz-final-project"
BQ_DATASET = "gharchive"
BQ_TABLE = "github_archive"

def get_push_events():
    query = f"""
        SELECT name AS repo_name, push_count
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        ORDER BY push_count DESC
        LIMIT 100
    """
    df = client.query(query).to_dataframe()
    df["url"] = df["repo_name"].apply(lambda name: f"http://github.com/{name}")
    df["link"] = df["url"].apply(lambda url: f'<a href="{url}" target="_blank">點我</a>')  # 👈 加上超連結
    return df

# 用戶界面
st.set_page_config(page_title="GitHub PushEvent 熱門 Repo", page_icon="📊", layout="wide")

# 美化標題
st.markdown(
    """
    <h1 style="text-align: center; color: #2D9CDB;">📈 GitHub PushEvent 熱門 Repo</h1>
    <p style="text-align: center; color: #7B7B7B;">這是根據 GitHub 的 PushEvent 統計最熱門的 repositories</p>
    """, unsafe_allow_html=True)

# 顯示資料
df = get_push_events()
if df.empty:
    st.write("沒有資料")
else:
    # 顯示柱狀圖
    st.bar_chart(df.set_index("repo_name")["push_count"], use_container_width=True)

    # 顯示表格（帶超連結）
    st.write("### Repository Details")
    df_display = df[["repo_name", "push_count", "link"]].rename(columns={
        "repo_name": "Repository",
        "push_count": "Push 次數",
        "link": "連結"
    })

    # 👇 用 HTML 顯示表格，才能保留 <a href> 超連結
    st.write(df_display.to_html(escape=False, index=False), unsafe_allow_html=True)
