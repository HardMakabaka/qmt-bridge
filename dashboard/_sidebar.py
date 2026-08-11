"""共享侧边栏连接配置 — 所有页面 import 此模块以渲染连接 UI。"""

import streamlit as st


def render_sidebar():
    """渲染侧边栏连接配置并返回 client（可能为 None）。"""
    st.sidebar.title("QMT Bridge")
    host = st.sidebar.text_input("服务地址", value="127.0.0.1", key="_sb_host")
    port = st.sidebar.number_input(
        "端口", value=13543, min_value=1, max_value=65535, step=1, key="_sb_port"
    )
    api_key = st.sidebar.text_input(
        "API Key（交易功能需要）", type="password", key="_sb_api_key"
    )

    if st.sidebar.button("连接 / 刷新", key="_sb_connect"):
        try:
            from qmt_bridge import QMTClient

            client = QMTClient(host, int(port), api_key=api_key)
            health = client.health_check()
            st.session_state["client"] = client
            st.session_state["connected"] = True
            st.session_state["health"] = health
            st.sidebar.success("连接成功")
        except Exception as e:
            st.session_state["connected"] = False
            st.sidebar.error(f"连接失败: {e}")

    if st.session_state.get("connected"):
        st.sidebar.caption("🟢 已连接")
    else:
        st.sidebar.caption("🔴 未连接 — 请点击「连接 / 刷新」")

    return st.session_state.get("client")


def require_client():
    """渲染侧边栏并要求已连接。未连接时调用 st.stop()。"""
    client = render_sidebar()
    if not st.session_state.get("connected") or client is None:
        st.warning("请先在侧边栏配置连接。")
        st.stop()
    return client
