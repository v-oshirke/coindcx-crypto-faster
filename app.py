import streamlit as st
import requests
import pandas as pd
import numpy as np
import ta
import pytz
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="RSI Screener", layout="wide")
st.title("📉 CoinDCX RSI Screener (5-Min)")

# --- Round time to previous 5-minute block ---
def round_time_to_last_5_min(dt):
    return dt - timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)

# --- Get list of all USDT pairs from CoinDCX ---
@st.cache_data(ttl=300)
def get_usdt_pairs():
    url = "https://api.coindcx.com/exchange/v1/markets"
    response = requests.get(url)
    data = response.json()
    inr_pair = []
    for i in data:
        if i.endswith("USDT"):
            x = i[:-4] + "_" + i[-4:]
            inr_pair.append(x)
    return inr_pair

# --- Fetch data and compute RSI ---
def data_downloader(name, interval="5m"):
    url = f"https://public.coindcx.com/market_data/candles?pair=B-{name}&interval={interval}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()

        df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        if df.empty or len(df) < 20:
            return None

        df['date_time'] = df['time'].apply(lambda x: datetime.fromtimestamp(int(str(x)[:10]), tz=pytz.UTC).astimezone(pytz.timezone('Asia/Kolkata')))
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)

        df = df.sort_values(by='date_time').reset_index(drop=True)
        df['rsi'] = ta.momentum.rsi(df['close'], window=14)

        latest_row = df.iloc[-1]
        current_ist = round_time_to_last_5_min(datetime.now(pytz.timezone('Asia/Kolkata')))

        if latest_row['date_time'] < current_ist:
            return None

        return {
            "symbol": name,
            "datetime": latest_row['date_time'].strftime("%Y-%m-%d %H:%M:%S"),
            "rsi": round(latest_row['rsi'], 2) if pd.notna(latest_row['rsi']) else None
        }

    except Exception:
        return None

# --- Button to trigger scan ---
if st.button("🚀 Generate RSI Signals"):
    st.info("⏳ Fetching RSI data for all USDT pairs...")

    inr_pair = get_usdt_pairs()
    results = []

    with ThreadPoolExecutor(max_workers=30) as executor:
        future_to_symbol = {executor.submit(data_downloader, symbol): symbol for symbol in inr_pair}
        for future in as_completed(future_to_symbol):
            row = future.result()
            if row:
                results.append(row)

    df_result = pd.DataFrame(results)

    if not df_result.empty:
        df_result["signal"] = np.where(df_result["rsi"] <= 30, "BUY",
                              np.where(df_result["rsi"] >= 70, "SELL", "NEUTRAL"))
        df_filtered = df_result[df_result['signal'] != 'NEUTRAL'].sort_values("rsi")

        st.success(f"✅ {len(df_filtered)} RSI signals found as of {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}")
        st.dataframe(df_filtered.reset_index(drop=True), use_container_width=True)
    else:
        st.warning("⚠️ No valid RSI signals found for the latest 5-minute candle.")
