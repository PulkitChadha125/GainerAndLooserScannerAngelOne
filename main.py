import time
import traceback
import pandas as pd
import pyotp
from datetime import datetime, timedelta, timezone
import AngelIntegration
from Algofox import *
import csv
import os

BUYCE=False
BUYPE=False
tpce=False
tppe=False
slce=False
slpe=False

def write_to_order_logs(message):
    with open('OrderLog.txt', 'a') as file:  # Open the file in append mode
        file.write(message + '\n')


def round_down_to_interval(dt, interval_minutes):
    remainder = dt.minute % interval_minutes
    minutes_to_current_boundary = remainder
    rounded_dt = dt - timedelta(minutes=minutes_to_current_boundary)
    rounded_dt = rounded_dt.replace(second=0, microsecond=0)
    return rounded_dt

def determine_min(minstr):
    min=0
    if minstr =="ONE_MINUTE":
        min=1
    if minstr =="FIVE_MINUTE":
        min=5
    if minstr =="FIFTEEN_MINUTE":
        min=15
    if minstr =="THIRTY_MINUTE":
        min=30

    return min

result_dict_CE={}
result_dict_PE={}

def delete_file_contents(file_name):
    try:
        # Open the file in write mode, which truncates it (deletes contents)
        with open(file_name, 'w') as file:
            file.truncate(0)
        print(f"Contents of {file_name} have been deleted.")
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


def get_zerodha_credentials():
    credentials = {}
    try:
        df = pd.read_csv('ZerodhaCredentials.csv')
        for index, row in df.iterrows():
            title = row['Title']
            value = row['Value']
            credentials[title] = value
    except pd.errors.EmptyDataError:
        print("The CSV file is empty or has no data.")
    except FileNotFoundError:
        print("The CSV file was not found.")
    except Exception as e:
        print("An error occurred while reading the CSV file:", str(e))
    return credentials

next_specific_part_time=datetime.now()
credentials_dict = get_zerodha_credentials()
strategycode = credentials_dict.get('StrategyCode')
url = credentials_dict.get('algofoxurl')
username= credentials_dict.get('algofoxusername')
algofoxpassword=credentials_dict.get('algofoxpassword')
role= credentials_dict.get('ROLE')
strategytag=credentials_dict.get('strategytag')
createurl(url)
apikey=credentials_dict.get('apikey')
USERNAME=credentials_dict.get('USERNAME')
pin=credentials_dict.get('pin')
totp_string=credentials_dict.get('totp_string')
AngelIntegration.login(api_key=apikey,username=USERNAME,pwd=pin,totp_string=totp_string)
AngelIntegration.symbolmpping()
# print("NIFTY23MAY2422300CE: ",AngelIntegration.get_ltp(segment="NFO", symbol="NIFTY23MAY2422300CE", token=38750))
loginresult=login_algpfox(username=username, password=algofoxpassword, role=role)


if loginresult!=200:
    print("Algofoz credential wrong, shutdown down Trde Copier, please provide correct details and run again otherwise program will not work correctly ...")
    time.sleep(10000)


def calculate_percentage_change(previous_close, present_close):
    price_change = present_close - previous_close
    percentage_change = (price_change / previous_close) * 100
    return percentage_change
def get_user_settings():
    global result_dict_CE,result_dict_PE
    try:
        csv_path = 'TradeSettings_PE.csv'
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()
        result_dict_PE = {}
        for index, row in df.iterrows():
            # Create a nested dictionary for each symbol
            symbol_dict = {
                'Symbol': row['Symbol'],
                'Target': row['Target'],
                'Stoploss': row['Stoploss'],
                'SmallTF': row['SmallTF'],
                'BigTF': row['BigTF'],
                "cool": row['Sync'],
                "lotsize":row['lotsize'],
                "runtime": datetime.now(),
                "previousclose":None,"presentclose":None,
                'percentageChange': None,
                'PE_LTP':None,
                'TargetValue': None,
                'StoplossValue': None,
            }
            result_dict_PE[row['Symbol']] = symbol_dict

    except Exception as e:
        print("Error happened in fetching symbol PUT", str(e))
    try:
        csv_path = 'TradeSettings_CE.csv'
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()
        result_dict_CE = {}
        for index, row in df.iterrows():
            # Create a nested dictionary for each symbol
            symbol_dict = {
                'Symbol': row['Symbol'],
                'Target': row['Target'],
                'Stoploss':row['Stoploss'],
                'SmallTF': row['SmallTF'],
                'BigTF': row['BigTF'],
                "cool": row['Sync'],
                "lotsize": row['lotsize'],
                "runtime": datetime.now(),
                'percentageChange':None,"previousclose":None,"presentclose":None,
                'CE_LTP': None,
                'TargetValue': None,
                'StoplossValue': None,

            }
            result_dict_CE[row['Symbol']] = symbol_dict

    except Exception as e:
        print("Error happened in fetching symbol CALL", str(e))
get_user_settings()

def get_token(symbol):
    df= pd.read_csv("Instrument.csv")
    row = df.loc[df['symbol'] == symbol]
    if not row.empty:
        token = row.iloc[0]['token']
        return token


def get_strike(symbol):
    df = pd.read_csv("Instruments.csv")
    row = df.loc[df['symbol'] == symbol]
    if not row.empty:
        token = row.iloc[0]['strike']
        return token
def get_expiery(symbol):
    df = pd.read_csv("Instruments.csv")
    row = df.loc[df['symbol'] == symbol]
    if not row.empty:
        token = row.iloc[0]['expiry']
        return token
def get_basesymbol(symbol):
    df = pd.read_csv("Instruments.csv")
    row = df.loc[df['symbol'] == symbol]
    if not row.empty:
        token = row.iloc[0]['name']
        return token

def find_min_percentage_change(data):
    filtered_data = {k: v for k, v in data.items() if v['percentageChange'] is not None}
    min_entry = min(filtered_data.items(), key=lambda item: item[1]['percentageChange'])
    return min_entry
def find_max_percentage_change(data):
    filtered_data = {k: v for k, v in data.items() if v['percentageChange'] is not None}
    max_entry = max(filtered_data.items(), key=lambda item: item[1]['percentageChange'])
    return max_entry

def write_dict_to_csv(data_dict, filename):
    """Write a dictionary to a CSV file, clearing previous data.
    Parameters:
    data_dict (dict): The dictionary to write to the CSV file.
    filename (str): The name of the CSV file.
    """
    try:
        # Ensure the dictionary is not empty
        if not data_dict:
            raise ValueError("The dictionary is empty")

        # Extract field names from the first item
        fieldnames = list(data_dict[next(iter(data_dict))].keys())

        # Open the CSV file for writing (this will clear previous data)
        with open(filename, mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

            # Write the header
            writer.writeheader()

            # Write the rows
            for key, value in data_dict.items():
                writer.writerow(value)

        print(f"Dictionary successfully written to {filename}")

    except Exception as e:
        print(f"An error occurred while writing to CSV: {str(e)}")

def main_strategy ():
    global result_dict_CE,result_dict_PE,BUYCE,BUYPE,username, algofoxpassword, role,strategytag
    # CE
    try:
        for symbol, params in result_dict_CE.items():
            symbol_value = params['Symbol']
            timestamp = datetime.now()
            timestamp = timestamp.strftime("%d/%m/%Y %H:%M:%S")

            if isinstance(symbol_value, str):
                if datetime.now() >= params["runtime"]:
                    if params["cool"] == True:
                        time.sleep(int(3))
                    try:
                        print(get_token(params['Symbol']))
                        print(params['Symbol'])
                        print(params['BigTF'])
                        Bigdata = AngelIntegration.get_historical_data(symbol=params['Symbol'],
                                                                          timeframe=params['BigTF'],
                                                                       token=get_token(params['Symbol']),segment="NFO")
                    except Exception as e:
                        print("checking data again")
                        time.sleep(int(3))
                        Bigdata = AngelIntegration.get_historical_data(symbol=params['Symbol'],
                                                                       timeframe=params['BigTF'],
                                                                       token=get_token(params['Symbol']),segment="NFO")
                    last_three_rows = Bigdata.tail(3)
                    row2 = last_three_rows.iloc[1]
                    row1 = last_three_rows.iloc[2]
                    params['previousclose']=float(row2['close'])
                    params['presentclose']=float(row1['close'])

                    next_specific_part_time = datetime.now() + timedelta(seconds=determine_min(params["SmallTF"]) * 60)
                    next_specific_part_time = round_down_to_interval(next_specific_part_time,
                                                                     determine_min(params["SmallTF"]))
                    print("Next datafetch time = ", next_specific_part_time)
                    params['runtime'] = next_specific_part_time

                params['CE_LTP'] = AngelIntegration.get_ltp(symbol=params['Symbol'],token=get_token(params['Symbol']),segment='NFO')
                print("Contract: ",params['Symbol'])
                print("previousclose: ", params['previousclose'])
                print("presentclose: ", params['CE_LTP'])
                params['percentageChange'] = calculate_percentage_change(previous_close=params['previousclose'], present_close=params['CE_LTP'])
                print("percentageChange: ", params['percentageChange'])
        ce_contract_detail=find_max_percentage_change(result_dict_CE)
        symbol_max, details_max = ce_contract_detail
        print(f"Condition check for : {symbol_max},ltp: {AngelIntegration.get_ltp(symbol=symbol_max,token=get_token(symbol_max),segment='NFO')}")
        if (
            BUYCE==False
           ):
                BUYCE =True
                usedltp = AngelIntegration.get_ltp(symbol=symbol_max,token=get_token(symbol_max),segment='NFO')
                details_max['TargetValue'] = usedltp+details_max['Target']
                details_max['StoplossValue'] = usedltp-details_max['Stoploss']
                sname = f"{get_basesymbol(symbol_max)}|{str(get_expiery(symbol_max))}|{str(int(get_strike(symbol_max)))}|CE"
                Buy_order_algofox(symbol=sname, quantity=int(details_max['lotsize']), instrumentType="OPTIDX",
                                               direction="BUY", price=usedltp, product="MIS",
                                               order_typ="MARKET", strategy=strategytag,username=username,password=algofoxpassword,role=role)
                orderlog=f"{timestamp} Buy order executed call side @ {symbol_max} , @ {usedltp}, sl={details_max['StoplossValue'] },tp={details_max['TargetValue']}"
                print(orderlog)
                write_to_order_logs(orderlog)

        if BUYCE == True and  details_max['TargetValue'] and details_max['StoplossValue']>0:
            usedltp = AngelIntegration.get_ltp(symbol=symbol_max, token=get_token(symbol_max), segment='NFO')
            print(f"{timestamp} usedltp: ", usedltp)
            print(f"{timestamp} details_max['TargetValue']: ", details_max['TargetValue'])
            if usedltp >= details_max['TargetValue'] and details_max['TargetValue'] > 0:
                BUYCE = False
                orderlog = f"{timestamp} Target executed call side @ {symbol_max} , @ {usedltp}"
                sname = f"{get_basesymbol(symbol_max)}|{str(get_expiery(symbol_max))}|{str(int(get_strike(symbol_max)))}|CE"
                Sell_order_algofox(symbol=sname, quantity=int(details_max['lotsize']), instrumentType="OPTIDX",
                                               direction="BUY", price=usedltp, product="MIS",
                                               order_typ="MARKET", strategy=strategytag,username=username,password=algofoxpassword,role=role)
                print(orderlog)
                write_to_order_logs(orderlog)

            if usedltp <= details_max['StoplossValue'] and details_max['StoplossValue'] > 0:
                BUYCE = False
                orderlog = f"{timestamp} Stoploss executed call side @ {symbol_max} , @ {usedltp}"
                sname = f"{get_basesymbol(symbol_max)}|{str(get_expiery(symbol_max))}|{str(int(get_strike(symbol_max)))}|CE"
                Sell_order_algofox(symbol=sname, quantity=int(details_max['lotsize']), instrumentType="OPTIDX",
                                               direction="BUY", price=usedltp, product="MIS",
                                               order_typ="MARKET", strategy=strategytag,username=username,password=algofoxpassword,role=role)
                print(orderlog)
                write_to_order_logs(orderlog)
                    # PE
        for symbol, params in result_dict_PE.items():
            symbol_value = params['Symbol']
            timestamp = datetime.now()
            timestamp = timestamp.strftime("%d/%m/%Y %H:%M:%S")
            if isinstance(symbol_value, str):
                if datetime.now() >= params["runtime"]:
                    if params["cool"] == True:
                        time.sleep(int(3))
                    try:
                        Bigdata = AngelIntegration.get_historical_data(symbol=params['Symbol'],
                                                                       timeframe=params['BigTF'],
                                                                       token=get_token(params['Symbol']),segment="NFO")
                    except Exception as e:
                        print("checking data again")
                        time.sleep(int(5))
                        Bigdata = AngelIntegration.get_historical_data(symbol=params['Symbol'],
                                                                       timeframe=params['BigTF'],
                                                                       token=get_token(params['Symbol']),segment="NFO")
                    last_three_rows = Bigdata.tail(3)
                    row2 = last_three_rows.iloc[1]
                    row1 = last_three_rows.iloc[2]
                    params['previousclose'] = float(row2['close'])
                    params['presentclose'] = float(row1['close'])

                    next_specific_part_time = datetime.now() + timedelta(seconds=determine_min(params["SmallTF"]) * 60)
                    next_specific_part_time = round_down_to_interval(next_specific_part_time,
                                                                     determine_min(params["SmallTF"]))
                    print("Next datafetch time = ", next_specific_part_time)
                    params['runtime'] = next_specific_part_time
                params['PE_LTP'] = AngelIntegration.get_ltp(symbol=params['Symbol'],token=get_token(params['Symbol']),segment='NFO')
                print("Contract: ", params['Symbol'])
                print("previousclose: ", params['previousclose'])
                print("presentclose: ", params['PE_LTP'])
                params['percentageChange'] = calculate_percentage_change(previous_close=params['previousclose'],
                                                                         present_close=params['PE_LTP'])
                print("percentageChange: ", params['percentageChange'])
        pe_contract_detail=find_max_percentage_change(result_dict_PE)
        symbol_min, details_min = pe_contract_detail
        print(f"Condition check for : {symbol_min},ltp: {AngelIntegration.get_ltp(symbol=symbol_min,token=get_token(symbol_min),segment='NFO')}")
        if (
                BUYPE==False
        ):
            usedltp = AngelIntegration.get_ltp(symbol=symbol_min,token=get_token(symbol_min),segment='NFO')
            BUYPE =True
            details_min['TargetValue'] = usedltp+details_min['Target']
            details_min['StoplossValue'] = usedltp-details_min['Stoploss']
            sname = f"{get_basesymbol(symbol_max)}|{str(get_expiery(symbol_max))}|{str(int(get_strike(symbol_max)))}|PE"
            Buy_order_algofox(symbol=sname, quantity=int(details_max['lotsize']), instrumentType="OPTIDX",
                                      direction="BUY", price=usedltp, product="MIS",
                                      order_typ="MARKET", strategy=strategytag, username=username, password=algofoxpassword,
                                      role=role)
            orderlog=f"{timestamp} Buy order executed Put side @ {symbol_min} , @ {usedltp}, sl={details_min['StoplossValue'] },tp={details_min['TargetValue']}"
            print(orderlog)
            write_to_order_logs(orderlog)

        if BUYPE ==True and  details_min['TargetValue'] and details_min['StoplossValue']>0:
            usedltp= AngelIntegration.get_ltp(symbol=symbol_min,token=get_token(symbol_min),segment='NFO')
            print(f"{timestamp} usedltp: ", usedltp)
            print(f"{timestamp} details_min['TargetValue'] : ", details_min['TargetValue'] )
            if usedltp>=details_min['TargetValue'] and details_min['TargetValue']>0:
                BUYPE=False
                sname = f"{get_basesymbol(symbol_max)}|{str(get_expiery(symbol_max))}|{str(int(get_strike(symbol_max)))}|PE"
                Sell_order_algofox(symbol=sname, quantity=int(details_max['lotsize']), instrumentType="OPTIDX",
                                          direction="BUY", price=usedltp, product="MIS",
                                          order_typ="MARKET", strategy=strategytag, username=username, password=algofoxpassword,
                                          role=role)
                orderlog = f"{timestamp} Target executed Put side @ {symbol_min} , @ {usedltp}"
                print(orderlog)
                write_to_order_logs(orderlog)

            if usedltp<=details_min['StoplossValue'] and details_min['StoplossValue']>0:
                BUYPE = False
                sname = f"{get_basesymbol(symbol_max)}|{str(get_expiery(symbol_max))}|{str(int(get_strike(symbol_max)))}|PE"
                Sell_order_algofox(symbol=sname, quantity=int(details_max['lotsize']), instrumentType="OPTIDX",
                                           direction="BUY", price=usedltp, product="MIS",
                                           order_typ="MARKET", strategy=strategytag, username=username, password=algofoxpassword,
                                           role=role)
                orderlog = f"{timestamp} Stoploss executed Put side @ {symbol_min} , @ {usedltp}"
                print(orderlog)
                write_to_order_logs(orderlog)

        write_dict_to_csv(data_dict=result_dict_CE, filename="CE.csv")
        write_dict_to_csv(data_dict=result_dict_PE, filename="PE.csv")

    except Exception as e:
        print("Error happened in Main strategy loop: ", str(e))
        traceback.print_exc()



# AngelIntegration.get_ltp(segment="NFO",symbol="",token)


while True:
    main_strategy()
    time.sleep(1)









