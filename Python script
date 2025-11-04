import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# -----------------------
# Configuration
# -----------------------
CSV_MASTER = "splits_master.csv"  # stores all historical splits

NASDAQ_URL = "https://www.nasdaq.com/market-activity/stock-splits"
NYSE_URL = "https://www.nyse.com/listings/stock-splits"
SEC_EDGAR_RSS = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom"  # filter later

HEADERS = {"User-Agent": "Mozilla/5.0"}

# -----------------------
# Scrape Nasdaq
# -----------------------
def scrape_nasdaq():
    r = requests.get(NASDAQ_URL, headers=HEADERS)
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    data = []
    if table:
        for row in table.find_all("tr")[1:]:
            cols = [c.text.strip() for c in row.find_all("td")]
            if len(cols) >= 3:
                ticker, ratio, date = cols[0], cols[1], cols[2]
                data.append({"Ticker": ticker, "Ratio": ratio, "Date": date, "Source": "Nasdaq"})
    return pd.DataFrame(data)

# -----------------------
# Scrape NYSE
# -----------------------
def scrape_nyse():
    r = requests.get(NYSE_URL, headers=HEADERS)
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    data = []
    if table:
        for row in table.find_all("tr")[1:]:
            cols = [c.text.strip() for c in row.find_all("td")]
            if len(cols) >= 3:
                ticker, ratio, date = cols[0], cols[1], cols[2]
                data.append({"Ticker": ticker, "Ratio": ratio, "Date": date, "Source": "NYSE"})
    return pd.DataFrame(data)

# -----------------------
# Scrape SEC EDGAR RSS
# -----------------------
def scrape_sec():
    r = requests.get(SEC_EDGAR_RSS, headers=HEADERS)
    soup = BeautifulSoup(r.text, "xml")
    entries = soup.find_all("entry")
    data = []
    for entry in entries:
        title = entry.find("title").text
        if "reverse stock split" in title.lower():
            link = entry.find("link")["href"]
            date = entry.find("updated").text[:10]
            ticker = title.split()[0]
            data.append({"Ticker": ticker, "Ratio": "Unknown", "Date": date, "Source": "SEC", "Link": link})
    return pd.DataFrame(data)

# -----------------------
# Compare with historical CSV
# -----------------------
def get_new_entries(df):
    try:
        master_df = pd.read_csv(CSV_MASTER)
    except FileNotFoundError:
        master_df = pd.DataFrame()
    if master_df.empty:
        new_entries = df
    else:
        new_entries = df.merge(master_df, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
    # Append new entries to master
    if not new_entries.empty:
        combined_master = pd.concat([master_df, new_entries], ignore_index=True)
        combined_master.to_csv(CSV_MASTER, index=False)
    return new_entries

# -----------------------
# Save daily new entries to CSV
# -----------------------
def save_daily_csv(df):
    if df.empty:
        print("No new reverse splits today.")
        return
    filename = f"reverse_splits_{datetime.now().strftime('%Y-%m-%d')}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} new entries to {filename}")

# -----------------------
# Main Execution
# -----------------------
if __name__ == "__main__":
    nasdaq_df = scrape_nasdaq()
    nyse_df = scrape_nyse()
    sec_df = scrape_sec()

    combined_df = pd.concat([nasdaq_df, nyse_df, sec_df], ignore_index=True)
    new_entries = get_new_entries(combined_df)
    save_daily_csv(new_entries)
