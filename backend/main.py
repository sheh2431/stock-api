
from fastapi import FastAPI, HTTPException
import yfinance as yf
import pandas as pd
import sqlite3
from datetime import datetime, timedelta

app = FastAPI()

# SQLite 資料庫
DB_FILE = "stocks.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            date TEXT NOT NULL,
            close_price REAL NOT NULL,
            volume INTEGER NOT NULL,
            UNIQUE(symbol, date)
        )
    ''')
    conn.commit()
    conn.close()

init_db()

@app.get("/stocks/{symbol}/prices")
def get_stock_prices(symbol: str, days: int = 30):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("SELECT date, close_price, volume FROM stock_prices WHERE symbol = ? ORDER BY date DESC LIMIT ?", (symbol, days))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return {"message": f"No data found for {symbol}"}

    return {"symbol": symbol, "prices": [{"date": row[0], "close": row[1], "volume": row[2]} for row in rows]}

@app.get("/stocks/{symbol}/download")
def download_stock_data(symbol: str):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("SELECT date, close_price, volume FROM stock_prices WHERE symbol = ? ORDER BY date DESC", (symbol,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="No data available")

    df = pd.DataFrame(rows, columns=["Date", "Close Price", "Volume"])
    csv_file = f"{symbol}_historical_data.csv"
    df.to_csv(csv_file, index=False)

    return {"message": "CSV generated successfully", "file": csv_file}

@app.post("/stocks/update")
def update_stock_prices():
    dow30_symbols = [
        "AAPL", "MSFT", "JPM", "V", "PG", "JNJ", "DIS", "KO", "MCD", "GS",
        "IBM", "CAT", "TRV", "MMM", "CSCO", "XOM", "CVX", "WBA", "VZ", "BA",
        "NKE", "HD", "AXP", "MRK", "UNH", "PFE", "INTC", "WMT", "DOW", "AMGN"
    ]

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)

    for symbol in dow30_symbols:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="7d")

        for index, row in hist.iterrows():
            date = index.strftime("%Y-%m-%d")
            close_price = row["Close"]
            volume = row["Volume"]

            cursor.execute("INSERT OR IGNORE INTO stock_prices (symbol, date, close_price, volume) VALUES (?, ?, ?, ?)",
                           (symbol, date, close_price, volume))

    conn.commit()
    conn.close()

    return {"message": "Stock prices updated successfully"}
