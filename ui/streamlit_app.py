"""Internal analyst console - a thin client of the FastAPI service.

Baseline behaviour (ported from the original ``src/dashboard.py``): a Streamlit
UI with Plotly candlestick + volume subplots, 20/50-day SMA overlays,
fundamentals, news, and an on-demand AI briefing.

Milestone 4 reframes it as a thin client. It no longer reads CSVs or Delta
tables off disk and no longer imports the RAG/LLM code to run inference
in-process; chart data, indicators, lakehouse health, and briefings all come
from the API over HTTP (``ui/api_client.py``). Two consequences worth noting:

- The chart now plots the *curated* Delta table - the SMA lines are the ones
  PySpark computed in the batch job, not recomputed here. Before M4 the console
  read ``data/raw/*.csv`` and did its own rolling means, so it never actually
  showed the lakehouse's output.
- Fundamentals and the news feed still call Yahoo Finance directly. They're
  presentation-only lookups that never enter the pipeline or the Delta tables,
  so routing them through the API would add a hop without adding value.

Run the API first (``uvicorn src.api.main:app --reload``), then
``streamlit run ui/streamlit_app.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots

# `streamlit run` puts this file's own directory on sys.path, not the repo
# root - add it explicitly so `from src...`/`from ui...` resolve regardless of
# how or where streamlit is invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ui.api_client import (  # noqa: E402
    ApiError,
    create_briefing,
    get_lakehouse_stats,
    get_prices,
    get_tickers,
)


def _escape_markdown_math(text: str) -> str:
    """Stop Streamlit rendering currency amounts as LaTeX.

    Streamlit renders markdown, and markdown treats ``$...$`` as inline maths.
    A briefing that mentions two prices - "around the $510 level ... the $500
    mark" - has its entire middle swallowed into a formula: italicised, with
    every space stripped. It looks like the model emitted mangled text when in
    fact the text was fine and the renderer mangled it.

    Escaping the dollar signs is the whole fix. Applied at the point of display
    rather than in the generator, because the raw text is correct and is also
    written to data/briefings/ and re-indexed for retrieval - it should stay
    unescaped there.
    """
    return text.replace("$", r"\$")


st.set_page_config(page_title="Institutional Portfolio Dashboard", layout="wide")
st.title("INSTITUTIONAL PORTFOLIO DASHBOARD")
st.markdown(
    "Select a security from your portfolio to view market action, detailed "
    "financials, and generate an AI-driven briefing."
)

# --- Everything below this point comes from the API, not the local filesystem ---

try:
    tickers = get_tickers()
except ApiError as exc:
    st.error(str(exc))
    st.stop()

with st.sidebar:
    st.subheader("Lakehouse status")
    try:
        for table in get_lakehouse_stats():
            if table["available"]:
                st.write(f"**{table['name']}** — {table['rows']:,} rows (v{table['version']})")
            else:
                st.write(f"**{table['name']}** — not created yet")
    except ApiError as exc:
        st.warning(str(exc))

selected_ticker = st.selectbox("Select Security:", tickers)

try:
    payload = get_prices(selected_ticker, limit=100)
except ApiError as exc:
    st.error(str(exc))
    st.stop()

df = pd.DataFrame(payload["bars"])
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

st.subheader(f"MARKET ACTION: {selected_ticker}")
st.caption(
    f"{payload['rows']} curated bars from `{payload['source']}` "
    f"(Delta version {payload['table_version']}) — SMA lines computed by the Spark batch job."
)

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
                top_line = top_line.map(
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
            publisher = (
                provider.get("displayName", "Unknown source")
                if isinstance(provider, dict)
                else "Unknown source"
            )

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
    with st.spinner("Calling the API (retrieval + local inference)..."):
        try:
            result = create_briefing(selected_ticker, bars=5)
            st.write(_escape_markdown_math(result["text"]))
            if result["retrieved_sources"]:
                st.caption(f"Grounded in: {', '.join(result['retrieved_sources'])}")
            else:
                st.caption("No prior context retrieved (index empty or not yet built).")
            st.caption(
                f"retrieval {result['retrieval_latency_seconds']}s · "
                f"generation {result['generation_latency_seconds']}s"
            )
        except ApiError as exc:
            st.error(str(exc))
