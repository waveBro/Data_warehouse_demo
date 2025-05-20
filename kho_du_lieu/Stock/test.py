import requests
import json
import os
from dotenv import load_dotenv
from alpha_vantage.timeseries import TimeSeries  # timeseries provide data for stock market
from alpha_vantage.fundamentaldata import FundamentalData  # fundamentaldata provide data for stock market
import pandas as pd
# để lấy dữ liệu qua API -> cần API key

# save api key into .env file
# get api key from .env file

# load .env file
load_dotenv()

# get api key from .env file
API_KEY = os.getenv("API_KEY")
fd = FundamentalData(key = API_KEY, output_format = "pandas")

# file.csv hold symbol of stock
path = "C:/Users/vanphuc computer/Downloads/nasdaq_screener_1746356261122.csv"
df = pd.read_csv(path)
symbols = df["Symbol"].tolist()  # convert column Symbol to list


data_path = "C:/Users/vanphuc computer/VietLe-beef/kho_du_lieu/Stock/Data"
dataframe = pd.json_normalize("file.json", "name of key ")

data = []
for symbol in symbols:
    try:
        company, _ = fd.get_company_overview(symbol = symbol) 
        data.append(company)  # append company data to data list        
        # convert data to csv file
        df = pd.DataFrame(data)  # convert data to dataframe
        # save into folder Data
        df.to_csv(f"{data_path}/company_overview.csv", index = False)  # save data to csv file
    except Exception as e:  
        print(f"Error {e}")
        continue