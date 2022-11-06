import os
import time
import json
import traceback
from datetime import datetime

import requests
import pandas as pd
from pandas import json_normalize
from tqdm import tqdm

cur_dir = os.path.abspath(os.path.dirname(__file__))
os.chdir(cur_dir)

indices = ["NIFTY","BANKNIFTY"]

def init():
    global session
    session = requests.Session()

def get_symbols():
    
    headers = {
        'authority': 'www.nseindia.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36',
        'accept': '*/*',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://www.nseindia.com/option-chain',
        'accept-language': 'en-US,en;q=0.9,lb;q=0.8',
    #     'cookie': '_ga=GA1.2.1294292226.1595302388; _gid=GA1.2.670341467.1596293142; ak_bmsc=2E345A653E860343A86F85439D9B6E9B0212D6572C2D0000E22B265F2FC9F562~ploOTtThYOwrmn4DvbfROVbDNX3c3w77eer0w0689t4XGf4cy5NW04AE2bsGXhiT/x18r5c0IrQBnltCAKDMOq6C/1pIvUIFua3vsZkIzNbXCXCHzf2LICcGQRW7kPCIER2Vum/3UJHyMnSu4R3+WBvz2SI9r3VU4HezxjVwPi/4iihJziY2M13eO3dMfncrLOHhswuCt/Pk1WLi/rhI40x3VmhVIFIQCy5u/JkJHv0kNfInv1kzOckS1zU1Uc2h5S; RT=^\\^z=1^&dm=nseindia.com^&si=89540d29-b68b-41fa-bca2-f09e6ae51d0b^&ss=kdchwfqt^&sl=1^&tt=2yk^&bcn=^%^2F^%^2F684dd30a.akstat.io^%^2F^\\^; _gat_UA-143761337-1=1; bm_sv=19F5BA65A60281B23A44FDA4DA8EC0D5~NQwnEsHxdHDWnQ6xkERRHqJ0rUVOIbip0cT6/mM4qj2GqUQtgLRxcR11QgoRvwfIaMAvjzkr+j4Byuq6ngWfTxFZooea1m/Pkz6q155halGmn8gKvvfqV0yCedfIRFxtYm5tsWZ+eTm8uI3l2E4h214jj5OoD+JZeLv2Jt4HGMY=',
    }

    response = session.get('https://www.nseindia.com/api/master-quote', headers=headers)
    return response.json() + indices


def get_data(symbol):
    try:
        equity_url = 'https://www.nseindia.com/api/option-chain-equities'
        indices_url = 'https://www.nseindia.com/api/option-chain-indices'

        headers = {
            'authority': 'www.nseindia.com',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36',
            'accept': '*/*',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.nseindia.com/option-chain',
            'accept-language': 'en-US,en;q=0.9,lb;q=0.8',
        # 'cookie': '_ga=GA1.2.1294292226.1595302388; bm_mi=401BF84B235E7DDD148DB2CF207AAF27~oHhVp+pmL67MpXEnpg9eFBw0IajemkjAGbGU+Udg68FpTOSCcux7L4nBzpo0DOMX+fwSDqj3InUSNU87pzbA3jsDRaIS2cB9kYlihF16V/6zLBsguoOGeWM254Nuwak4cYPgfqD1TUMSEYwdqtBOPgVgI+6EkLTzBTNour2WebokhDFQf393RCAlchj9XfiMcC6g+zB60DmIeA/2JDBwlRHbMppwhkkS1OeyNgrNWFs8i7Fu/cPPgQE6KA1vmWMLg7XIRfjKpsgG561LyVOOtb7+2KzrsR3R7Pk0JTyoVnM=; _gid=GA1.2.670341467.1596293142; ak_bmsc=40C0BDAAC6E11965275973503650ED83B854E9EC842B0000997F255F22CB5161~plKTiIdufjimvL5MrVFoMleqStQpLBX9L4ke+uTMLxfms+Lqe+oj20pjZ6RTfZEZQ0L52snRxmNheZ/8ShCdLbMeeyXGxNJ9iJEJXm0gW6hUs8C0UWC9b76+wmI2gj2mF8VxS2AGHTtI4upcQTS+ZBtCIJs8yNOIgOpswKpmV5aQWVKEcc/UMA/PYh7dAP0r1xf8PROjx6Pw9uj7FHZhj3UQImslSZeu7GxkDAPvw+zm4RT7Crp0/ZK38PbibtjeW1; RT="z=1&dm=nseindia.com&si=89540d29-b68b-41fa-bca2-f09e6ae51d0b&ss=kdbrpygb&sl=6&tt=6dz&bcn=%2F%2F684d0d3f.akstat.io%2F&obo=1&ld=g3ui"; bm_sv=DDF5AD1ACD28F9B9B1859525E6D9A77C~9ZbC3N8GZCor4+CM4pkuXZ0kj/S1UrUIudMagTnLRWHGh7VppBqiV2EqLbIgRWNaVUbANIqzKdfj4sl4zXvkdFL/XXUhri0JKTdGikgyTgfksFS6ByMn1l/yYcKU1/EIdMY7pdSsW3CC3RwetjvwAmE+Frc7zrQlRW3Zhu8xLUQ=',
        }

        params = (
            ('symbol', symbol),
        )
        time.sleep(.5)
        
        if symbol in indices:
            response = session.get(indices_url, headers=headers, params=params)
        else:
            response = session.get(equity_url, headers=headers, params=params)

        result = response.json()
        datas = result["records"]["data"]
        return datas

    except Exception as e:
        print("ERROR:get_data")
        return []


def process(symbol,expiry_date):

    col_map = {
        "openInterest":"OI",
        "changeinOpenInterest" : "CHNG IN OI",
        "totalTradedVolume" : "VOLUME",
        "impliedVolatility" : "IV",
        "lastPrice" : "LTP",
        "change" :"CHNG", 
        "bidQty" : "BID QTY",
        "bidprice" : "BID PRICE",
        "askPrice" : "ASK PRICE",
        "askQty" : "ASK QTY",
        "strikePrice" : "STRIKE PRICE", 
    }
    pe_data_order = ["BID QTY","BID PRICE","ASK PRICE","ASK QTY","CHNG","LTP","IV","VOLUME","CHNG IN OI","OI"]

    try:
        datas = get_data(symbol)
        datas = [data for data in datas if data.get("expiryDate") == expiry_date]
        df = pd.DataFrame(datas)
        df['CE']  = df['CE'].apply(lambda x : {} if pd.isna(x) else x)
        df['PE']  = df['PE'].apply(lambda x : {} if pd.isna(x) else x)
        ce_data = json_normalize(df['CE'])
        ce_data = ce_data[col_map.keys()]
        ce_data = ce_data.rename(columns=col_map)
        if symbol in ["NIFTY"]:
            ce_data[["OI" , "CHNG IN OI"]] = ce_data[["OI" , "CHNG IN OI"]].apply(lambda x : x.mul(75))
        ce_data.columns = pd.MultiIndex.from_tuples([("CE", col) for col in ce_data.columns],names=("code","cols"))
        
        """FOR PE SAME PROCESS AS CE..."""
        pe_data = json_normalize(df['PE'])
        pe_data = pe_data[col_map.keys()]
        pe_data.rename(columns=col_map,inplace=True)

        if symbol in ["NIFTY"]:
            pe_data[["OI" , "CHNG IN OI"]] = pe_data[["OI" , "CHNG IN OI"]].apply(lambda x : x.mul(75))
            
        pe_data = pe_data[pe_data_order]
        pe_data.columns = pd.MultiIndex.from_tuples([("PE", col) for col in pe_data.columns],names=("code","cols"))
        
        df_final = pd.merge(ce_data,pe_data,left_index=True,right_index=True)
        return df_final
    
    except Exception as e:
        # print("ERROR:process")
        # print(traceback.format_exc())
        return pd.DataFrame()

def write_to_sheet(df_dict,filename):
    with pd.ExcelWriter(filename) as writer:
        for key in df_dict:
            df_dict[key].to_excel(writer,sheet_name=key)

def write_to_file(dfs):
    current_date = datetime.now().strftime("%Y_%m_%d")
    
    os.makedirs("New_Option_Data",exist_ok=True)
    for key in tqdm(dfs):
        filename = os.path.join("New_Option_Data",key+".xlsx")
        if os.path.exists(filename):
            df_symbol_dict = pd.read_excel(filename,sheet_name=None,header=[0,1],index_col=0)
            df_symbol_dict[current_date] = dfs[key]
        else:
            df_symbol_dict = {}
            df_symbol_dict[current_date] = dfs[key]

        write_to_sheet(df_symbol_dict,filename)
             
def main(expiry_date):
    symbols = get_symbols()
    dfs = {}
    print("Extracting Data")
    for symbol in tqdm(symbols):
        dfs[symbol] = process(symbol,expiry_date)
        
    ## dfs = {symbol : process(col,expiry_date) for symbol in symbols}
    print("Write to file")
    write_to_file(dfs)

if __name__ == "__main__":
    expiry_date = input("Enter the expiry code(follow format: 24-Nov-2022): ")
    import datetime
  
    try:
        datetime_object = datetime.datetime.strptime(expiry_date, '%d/%m/%y')
        init()
        main(expiry_date)
    except ValueError as e:
        print('Please type correct format:', e)


