from datetime import datetime, timedelta
import pandas as pd
import requests
import time
import json
import csv
import ast
import os

# Load credentials
CREDENTIALS = "Credentials/kore_credentials.json"

def check_credentials():
    #Check all the data in the json file
    try:
        with open(CREDENTIALS, "r") as file:
            credentials = json.load(file)
    except FileNotFoundError:
        print(f"The file was not found: {CREDENTIALS}")
        exit(1)
    except json.JSONDecodeError:
        print(f"Error with the json: {CREDENTIALS}")
        exit(1)

    host = credentials.get("host")
    account_id = credentials.get("account_id")
    token = credentials.get("jwt_token")
    bot_id = credentials.get("app_id")

    if not all([host, account_id, token]):
        print("There's missing fields in the json")
        exit(1)

    return(host, account_id,token,bot_id)

def get_users_status(host, account_id, token,bot_id):
    url = f"https://{host}/agentassist/api/public/analytics/1.1/account/{account_id}//userstatus"

    date = datetime.now()
    date = date.strftime("%Y-%m-%d")

    headers = {
    "Content-Type": "application/json",
    "auth": token,
    "User-Agent": "PostmanRuntime/7.42.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Iid": bot_id
    }

    body = {
    "filter": {
        "dateFilter": {
            "startDate":date,
            "endDate": date,
            "timeZoneOffSet": -330
                    }
                },
            "granularity": "PT30M"
        }


    csv_filename = f"agent_status_houtly.csv"
    is_new_file = not os.path.exists(csv_filename)

    all_data = []
    offset = 0
    request_count = 0

    while True:
        url = url.format(offset=offset)
        
        try:
            response = requests.post(url, headers=headers, json=body)
            
            
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting for 60 seconds...")
                time.sleep(60)
                continue
            response.raise_for_status()
            
            data = response.json()
            
            #Check the data
            if "data" in data and isinstance(data["data"], list):
                filtered_data = [item for item in data["data"] if "timeInterval" in item and item["timeInterval"]]
                
                print(f"Fetched {len(filtered_data)} records with status. Total offset: {offset}.")
                
                #Save the data in a csv
                if filtered_data:
                    with open(csv_filename, mode="a", newline="", encoding="utf-8") as csv_file:
                        writer = csv.writer(csv_file)
                        
                        #Headers
                        if is_new_file:
                            writer.writerow(filtered_data[0].keys())
                            is_new_file = False
                        
                        #Rows
                        for row in filtered_data:
                            writer.writerow(row.values())
            else:
                print("No 'data' field in response or no more records.")
                break
            
            #Check the "hasMore" tag
            if not data.get("hasMore", False):
                break
            
            offset += data.get("limit", 50)  
            request_count += 1
            
            #After 60 request it pause
            if request_count >= 60:
                print("Reached 60 API calls. Pausing for 60 seconds...")
                time.sleep(60)
                request_count = 0
        
        except requests.RequestException as e:
            print(f"Error during request: {e}")
            break
    
    print(f"Data collection complete. Records with status saved incrementally to {csv_filename}.")



host,account_id,token,bot_id=check_credentials()
get_users_status(host,account_id,token,bot_id)