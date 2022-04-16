#import all needed modules
import requests
import json
import pandas as pd
import csv

#Import json data from API
url = "https://api.coinlore.net/api/tickers/"
response = requests.get(url)
print(response)
df=json.loads(response.text)
df2=pd.DataFrame(df["data"])

#Write parsed json data to csv 
df2=df2.set_index("id")
df2.to_csv("bitcoin.csv", sep=',' ,escapechar='\\', quoting=csv.QUOTE_NONE, encoding='utf-8' )
