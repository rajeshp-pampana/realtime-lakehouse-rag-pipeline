"""Internal analyst console.

Baseline behaviour (ported from the original ``src/dashboard.py``): a Streamlit UI
with Plotly candlestick + volume subplots, 20/50-day SMA overlays, fundamentals,
news, and an on-demand AI briefing.

Milestone 4 reframes this as a thin client: it stops reading local files and calls
the FastAPI service (``src/api/main.py``) for chart data, metrics, and briefings, so
the UI and API start/stop independently.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots

# `streamlit run` puts this file's own directory on sys.path, not the repo
# root - add it explicitly so `from src...` resolves regardless of how/where
# streamlit is invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.llm.briefing_generator import generate_briefing  # noqa: E402

st.set_page_config(page_title="Institutional Portfolio Dashboard", layout="wide")
st.title("INSTITUTIONAL PORTFOLIO DASHBOARD")
st.markdown(
    "Select a security from your portfolio to view market action, detailed "
    "financials, and generate an AI-driven briefing."
)

TICKERS = [
    "MSFT", "CRWD", "AVGO", "GLE.PA", "NVDA", "AMZN", "AXON", "PANW",
    "INTC", "NOW", "IREN", "GOOG", "MU", "SOFI", "PLTR", "RDW", "DRAM",
]

selected_ticker = st.selectbox("Select Security:", TICKERS)

today_str = datetime.now().strftime("%Y%m%d")
file_path = f"data/raw/{selected_ticker}_raw_{today_str}.csv"

try:
    df = pd.read_csv(file_path)
    if "Price" in df.columns or "Ticker" in df.columns or "Unnamed" in str(df.columns[0]):
        df = pd.read_csv(
            file_path, skiprows=3, names=["Date", "Close", "High", "Low", "Open", "Volume"]
        )

    for col in ("Close", "Open", "High", "Low", "Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Close", "Date"])

    df["SMA_20"] = df["Close"].rolling(window=20, min_periods=1).mean()
    df["SMA_50"] = df["Close"].rolling(window=50, min_periods=1).mean()

    st.subheader(f"MARKET ACTION: {selected_ticker}")

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=(f"{selected_ticker} Price (USD)", "Volume"),
        row_width=[0.2, 0.7],
    )
    fig.add_trace(
        go.Candlestick(
            x=df["Date"], open=df["Open"], high=df["High"],
            low=df["Low"], close=df["Close"], name="Price",
        ),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["Date"], y=df["SMA_20"], line=dict(color="orange", width=1.5), name="20-Day SMA"),
        row=1, col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["Date"], y=df["SMA_50"], line=dict(color="blue", width=1.5), name="50-Day SMA"),
        row=1, col=1,
    )
    fig.add_trace(
        go.Bar(x=df["Date"], y=df["Volume"], name="Volume", marker_color="rgba(128, 128, 128, 0.5)"),
        row=2, col=1,
    )
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        height=600,
        margin=dict(l=0, r=0, t=30, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("FINANCIAL METRICS & EARNINGS SURPRISE")

    with st.spinner("Fetching institutional data..."):
        ticker_data = yf.Ticker(selected_ticker)
        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("**Analyst Consensus & Price Targets**")
            try:
                info = ticker_data.info
                rec = info.get("recommendationKey", "N/A").upper()
                mean_target = info.get("targetMeanPrice", "N/A")
                high_target = info.get("targetHighPrice", "N/A")
                low_target = info.get("targetLowPrice", "N/A")

                st.write(f"- **Recommendation:** {rec}")
                st.write(f"- **Mean Target:** ${mean_target}")
                st.write(f"- **High Target:** ${high_target}")
                st.write(f"- **Low Target:** ${low_target}")
            except Exception:
                st.info("Analyst targets currently unavailable.")

        with col2:
            st.markdown("**Upcoming & Recent Earnings**")
            try:
                earnings_dates = ticker_data.get_earnings_dates(limit=4)
                if earnings_dates is not None and not earnings_dates.empty:
                    earnings_dates.index = earnings_dates.index.tz_localize(None)
                    disp_df = earnings_dates[["EPS Estimate", "Reported EPS", "Surprise(%)"]].copy()
                    disp_df = disp_df.astype(str)
                    disp_df = disp_df.replace({"nan": "Pending", "None": "Pending", "<NA>": "Pending"})
                    st.dataframe(disp_df)
                else:
                    st.info("No recent earnings surprise data available.")
            except Exception:
                st.info("Earnings calendar currently unavailable.")

        with col3:
            st.markdown("**Income Statement (Top-Line)**")
            try:
                income = ticker_data.income_stmt
                if income is not None and not income.empty:
                    target_rows = ["Total Revenue", "Gross Profit", "Operating Income", "Net Income"]
                    available_rows = [row for row in target_rows if row in income.index]
                    top_line = (
                        income.loc[available_rows].iloc[:, :2]
                        if available_rows
                        else income.head(4).iloc[:, :2]
                    )
                    top_line = top_line.applymap(
                        lambda x: f"${x / 1_000_000_000:,.2f}B" if pd.notnull(x) else "N/A"
                    )
                    st.dataframe(top_line)
                else:
                    st.info("Income statement data currently unavailable.")
            except Exception:
                st.info("Financial statements are temporarily rate-limited.")

    st.divider()

    st.subheader("RECENT CATALYSTS & NEWS FEED")
    try:
        news = ticker_data.news
        if news:
            for item in news[:3]:
                # yfinance nests news details inside a 'content' dictionary.
                content = item.get("content", item)
                title = content.get("title", "No title")

                provider = content.get("provider", {})
                publisher = provider.get("displayName", "Unknown source") if isinstance(provider, dict) else "Unknown source"

                click_through = content.get("clickThroughUrl", {})
                link = (
                    click_through.get("url", content.get("canonicalUrl", "#"))
                    if isinstance(click_through, dict)
                    else "#"
                )
                st.markdown(f"- **[{title}]({link})** ({publisher})")
        else:
            st.write("No recent news available.")
    except Exception:
        st.info("News feed currently unavailable.")

    st.divider()

    st.subheader("AI QUANTITATIVE BRIEFING")
    if st.button(f"Generate Briefing for {selected_ticker}"):
        with st.spinner("Retrieving context and running local inference..."):
            try:
                result = generate_briefing(selected_ticker, df.tail(5))
                st.write(result["text"])
                if result["retrieved_sources"]:
                    st.caption(f"Grounded in: {', '.join(result['retrieved_sources'])}")
                else:
                    st.caption("No prior context retrieved (index empty or not yet built).")
                st.caption(
                    f"retrieval {result['retrieval_latency_seconds']}s · "
                    f"generation {result['generation_latency_seconds']}s"
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"Inference failed. Error: {exc}")

except FileNotFoundError:
    st.error(
        f"No local data found for {selected_ticker}. Run "
        "'python -m src.ingestion.fetch_market_data' to update your local store."
    )
