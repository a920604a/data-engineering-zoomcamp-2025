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
        SELECT name AS repo_name
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
        ORDER BY push_count DESC
        LIMIT 100
    """
    df = client.query(query).to_dataframe()
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
    # 設定顏色並顯示柱狀圖
    st.bar_chart(df.set_index("repo_name"), use_container_width=True)

    # 美化表格
    st.write("### Repository Details")
    st.table(df.style.set_table_styles(
        [{
            'selector': 'thead th',
            'props': [('background-color', '#2D9CDB'), ('color', 'white'), ('text-align', 'center')],
        }, {
            'selector': 'tbody td',
            'props': [('text-align', 'center'), ('padding', '10px')],
        }]
    ))
   
