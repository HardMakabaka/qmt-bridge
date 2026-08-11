"""数据下载 — 历史下载任务、快捷下载。"""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _sidebar import require_client

st.set_page_config(page_title="数据下载 - QMT Bridge", layout="wide")
st.title("数据下载")

client = require_client()

# ── 历史下载任务 ──────────────────────────────────────────────────

st.header("历史下载任务")
st.caption("提交服务端历史 K 线下载任务，下载完成后可通过 get_local_data 快速读取。")

col1, col2 = st.columns(2)
with col1:
    dl_stocks = st.text_area(
        "股票代码（每行一个或逗号分隔）",
        value="000001.SZ\n600519.SH",
        height=120,
        key="dl_stocks",
    )
with col2:
    dl_period = st.selectbox("K 线周期", ["1d", "1w", "1m", "5m", "15m", "30m", "60m"], key="dl_period")
    dl_start = st.text_input("开始日期 (YYYYMMDD)", value="", key="dl_start")
    dl_end = st.text_input("结束日期 (YYYYMMDD)", value="", key="dl_end")
    dl_batch_size = st.number_input("每批股票数", min_value=1, max_value=100, value=10, step=1, key="dl_batch_size")
    dl_max_attempts = st.number_input("单股最大尝试次数", min_value=1, max_value=5, value=2, step=1, key="dl_max_attempts")

if st.button("提交历史下载任务", key="btn_history_download_job", type="primary"):
    codes = [c.strip() for line in dl_stocks.split("\n") for c in line.split(",") if c.strip()]
    if not codes:
        st.warning("请输入至少一个股票代码。")
    else:
        try:
            with st.spinner(f"正在提交 {len(codes)} 只股票的 {dl_period} 下载任务..."):
                result = client.create_history_download_job(
                    codes,
                    period=dl_period,
                    start_time=dl_start,
                    end_time=dl_end,
                    batch_size=int(dl_batch_size),
                    max_attempts=int(dl_max_attempts),
                )
            st.success(f"任务已提交: {result.get('job_id')}")
            st.json(result)
        except Exception as e:
            st.error(f"提交失败: {e}")

job_id = st.text_input("查询历史下载任务 ID", value="", key="download_job_id")
if st.button("查询任务状态", key="btn_get_download_job"):
    if not job_id.strip():
        st.warning("请输入任务 ID。")
    else:
        try:
            st.json(client.get_history_download_job(job_id.strip()))
        except Exception as e:
            st.error(f"查询失败: {e}")

if st.button("取消任务", key="btn_cancel_download_job"):
    if not job_id.strip():
        st.warning("请输入任务 ID。")
    else:
        try:
            st.json(client.cancel_history_download_job(job_id.strip()))
        except Exception as e:
            st.error(f"取消失败: {e}")

st.markdown("---")

# ── 快捷下载 ──────────────────────────────────────────────────────

st.header("快捷下载")
st.caption("一键触发服务端下载常用数据集。")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("下载板块数据", key="btn_dl_sector", use_container_width=True):
        try:
            with st.spinner("下载中..."):
                result = client.download_sector_data()
            st.success("板块数据下载完成")
            st.json(result)
        except Exception as e:
            st.error(f"下载失败: {e}")

    if st.button("下载指数权重", key="btn_dl_index", use_container_width=True):
        try:
            with st.spinner("下载中..."):
                result = client.download_index_weight()
            st.success("指数权重下载完成")
            st.json(result)
        except Exception as e:
            st.error(f"下载失败: {e}")

    if st.button("下载 ETF 信息", key="btn_dl_etf", use_container_width=True):
        try:
            with st.spinner("下载中..."):
                result = client.download_etf_info()
            st.success("ETF 信息下载完成")
            st.json(result)
        except Exception as e:
            st.error(f"下载失败: {e}")

    if st.button("下载节假日数据", key="btn_dl_holiday", use_container_width=True):
        try:
            with st.spinner("下载中..."):
                result = client.download_holiday_data()
            st.success("节假日数据下载完成")
            st.json(result)
        except Exception as e:
            st.error(f"下载失败: {e}")

with col2:
    if st.button("下载可转债数据", key="btn_dl_cb", use_container_width=True):
        try:
            with st.spinner("下载中..."):
                result = client.download_cb_data()
            st.success("可转债数据下载完成")
            st.json(result)
        except Exception as e:
            st.error(f"下载失败: {e}")

    if st.button("下载历史合约", key="btn_dl_contracts", use_container_width=True):
        try:
            with st.spinner("下载中..."):
                result = client.download_history_contracts()
            st.success("历史合约下载完成")
            st.json(result)
        except Exception as e:
            st.error(f"下载失败: {e}")

    if st.button("下载 IPO 数据", key="btn_dl_ipo", use_container_width=True):
        try:
            with st.spinner("下载中..."):
                result = client.download_ipo_data()
            st.success("IPO 数据下载完成")
            st.json(result)
        except Exception as e:
            st.error(f"下载失败: {e}")

with col3:
    if st.button("下载期权数据", key="btn_dl_option", use_container_width=True):
        try:
            with st.spinner("下载中..."):
                result = client.download_option_data()
            st.success("期权数据下载完成")
            st.json(result)
        except Exception as e:
            st.error(f"下载失败: {e}")


st.markdown("---")

# ── 财务数据下载 ──────────────────────────────────────────────────

st.header("财务数据下载")

fin_stocks = st.text_input(
    "股票代码（逗号分隔）",
    value="000001.SZ, 600519.SH",
    key="fin_dl_stocks",
)
fin_tables = st.multiselect(
    "报表类型",
    ["Balance", "Income", "CashFlow"],
    default=["Balance", "Income", "CashFlow"],
    key="fin_dl_tables",
)

if st.button("下载财务数据", key="btn_dl_financial"):
    codes = [c.strip() for c in fin_stocks.split(",") if c.strip()]
    if not codes:
        st.warning("请输入至少一个股票代码。")
    else:
        try:
            with st.spinner("下载中..."):
                result = client.download_financial_data2(codes, tables=fin_tables)
            st.success("财务数据下载完成")
            st.json(result)
        except Exception as e:
            st.error(f"下载失败: {e}")
