import requests
import pandas as pd
import re
import os
from datetime import datetime
import urllib3

# --- Configuration ---
URL = "https://www.cmegroup.com/delivery_reports/Silver_stocks.xls"
CSV_FILE = "comex_silver_master.csv"
XLS_FILENAME = f"Silver_Stocks.TEMP.xls"
HISTORIC_FOLDER = "historic"

COLUMNS = [
    "Activity Date", "Registered", "Regi. Daily Change", "Reg. Monthly Change", 
    "Reg. Monthly Change (In Millions)", "Eligible", "Total", "Daily Change", 
    "Month Change", "Month Change (in Millions)", "% Registered of Total", 
    "Total (In Millions)", "% of Start"
]


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_file(url, filename):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    print(f"Attempting to download from {url}...")
    
    # verify=False ignores SSL cert issues
    response = requests.get(url, headers=headers, verify=False, timeout=30)
    
    # Check if we got a successful status code
    response.raise_for_status()
    
    with open(filename, 'wb') as f:
        f.write(response.content)
    print(f"Successfully downloaded: {filename}")




def parse_xls(filename):
    # Read without header to catch the Activity Date line
    df = pd.read_excel(filename, header=None)
    data_date = None
    
    # 1. Row-by-row Date Search
    for _, row in df.iterrows():
        row_str = " ".join(row.astype(str))
        if "Activity Date:" in row_str:
            date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", row_str)
            if date_match:
                data_date = date_match.group(1)
                break

    # 2. Value Extraction with 3-decimal rounding
    def get_clean_val(label):
        mask = df.astype(str).apply(lambda x: x.str.contains(label, case=False, na=False)).any(axis=1)
        try:
            val = df[mask].iloc[0, 7]
            # Strip commas/symbols and round to 3 places
            clean_num = float(str(val).replace(',', '').replace('$', '').strip())
            return round(clean_num, 3)
        except (IndexError, ValueError):
            return 0.000

    registered = get_clean_val("TOTAL REGISTERED")
    eligible = get_clean_val("TOTAL ELIGIBLE")
    total = get_clean_val("COMBINED TOTAL")

    return data_date, registered, eligible, total

def update_master_csv(data_date, registered, eligible, total):
    if not os.path.exists(CSV_FILE):
        # Initial row logic
        initial_reg, initial_total = registered, total
        last_reg, last_total = registered, total
        master_df = pd.DataFrame()
    else:
        master_df = pd.read_csv(CSV_FILE)
        initial_reg = float(master_df.iloc[0]['Registered'])
        initial_total = float(master_df.iloc[0]['Total'])
        last_reg = float(master_df.iloc[-1]['Registered'])
        last_total = float(master_df.iloc[-1]['Total'])

    # Calculations all rounded to 3 decimal places
    new_row = {
        "Activity Date": data_date,
        "Registered": round(registered, 3),
        "Regi. Daily Change": round(registered - last_reg, 3),
        "Reg. Monthly Change": round(registered - initial_reg, 3),
        "Reg. Monthly Change (In Millions)": round((registered - initial_reg) / 1_000_000, 3),
        "Eligible": round(eligible, 3),
        "Total": round(total, 3),
        "Daily Change": round(total - last_total, 3),
        "Month Change": round(total - initial_total, 3),
        "Month Change (in Millions)": round((total - initial_total) / 1_000_000, 3),
        "% Registered of Total": f"{round((registered / total) * 100, 2)}%" if total != 0 else 0,
        "Total (In Millions)": round(total / 1_000_000, 0),
        "% of Start": f"{round(((total / initial_total) * 100), )}%" if initial_total != 0 else 1.000
    }

    # Append and save
    master_df = pd.concat([master_df, pd.DataFrame([new_row])], ignore_index=True)
    master_df.to_csv(CSV_FILE, index=False)
    
    # Generate the requested filename format: Silver_Stocks.YY.DD.MM.xls
    # We parse the Activity Date (M/D/YYYY) to get the components
    dt_obj = datetime.strptime(data_date, "%m/%d/%Y")
    new_filename = dt_obj.strftime("Silver_Stocks.%y.%m.%d.xls")
    return new_filename



if __name__ == "__main__":
    try:
        print("Welcome to the COMEX Inventory Tracker")

        # download the latest file
        print("... downloading latest file ... ")
        download_file(URL, XLS_FILENAME)

        # parse the file
        print("... parsing file ...")
        d_date, reg, elig, tot = parse_xls(XLS_FILENAME)

        # check to see if the last line of the master file is equal to today
        if os.path.exists(CSV_FILE):
            master_df = pd.read_csv(CSV_FILE)
            if not master_df.empty:
                # Get the date from the last row of the "Activity Date" column
                last_entry_date = str(master_df.iloc[-1]['Activity Date']).strip()
                
                if last_entry_date == d_date.strip():
                    print(f"Data for {d_date} already exists in master. Exiting.")
                    # Clean up the temp file before leaving
                    os.remove(XLS_FILENAME)
                    exit()

        # update our master csv file
        print("... updating CSV ... ")
        final_name = update_master_csv(d_date, reg, elig, tot)

        # move our XLS file to its new date-based name
        print("... moving xls file ...")
        if os.path.exists(HISTORIC_FOLDER + "/" + final_name):
            os.remove(HISTORIC_FOLDER + "/" + final_name) # Clean up if file already exists
        os.rename(XLS_FILENAME, HISTORIC_FOLDER + "/"+ final_name)


        print("Done!")

    except Exception as e:
        print(f"Error: {e}")
