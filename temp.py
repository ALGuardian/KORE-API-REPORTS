import pandas as pd
from datetime import datetime, timedelta
import requests
import json

CREDENTIALS = "Credentials/kore_credentials.json"

def check_credentials():
    try:
        with open(CREDENTIALS, "r") as file:
            credentials = json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {CREDENTIALS}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in file: {CREDENTIALS}")
    
    required_keys = ["host", "account_id", "jwt_token", "app_id"]
    if not all(key in credentials for key in required_keys):
        raise ValueError("Missing required credentials in JSON file.")
    
    return credentials["host"], credentials["account_id"], credentials["jwt_token"], credentials["app_id"]

def process_response(data, processed_user_ids):
    flattened_data = []
    for record in data.get("data", []):
        user_id = record.get("userId")
        if user_id in processed_user_ids:
            continue
        processed_user_ids.add(user_id)
        
        base_info = {
            "userId": user_id,
            "firstName": record.get("firstName"),
            "lastName": record.get("lastName"),
            "email": record.get("email"),
            "customId": record.get("customId")
        }
        
        statuses = record.get("status", [])
        if statuses:
            for status in statuses:
                flattened_data.append({
                    **base_info,
                    "primaryStatus": status.get("primaryStatus"),
                    "secondaryStatus": status.get("secondaryStatus"),
                    "startTime": status.get("startTime"),
                    "endTime": status.get("endTime"),
                    "duration": status.get("duration")
                })
        else:
            # Si no hay estados, registrar el usuario sin subestados
            flattened_data.append(base_info)
    return flattened_data

def save_to_excel(data, filename):
    df = pd.DataFrame(data)
    excel_filename = filename.replace('.csv', '.xlsx')
    df.to_excel(excel_filename, index=False)
    print(f"Data saved to {excel_filename}")

def get_agent_status_details():
    host, account_id, token, bot_id = check_credentials()

    url = f"https://{host}/agentassist/api/public/analytics/account/{account_id}/agentstatusdetails?offset={{offset}}"
    headers = {
        "Content-Type": "application/json",
        "auth": token,
        "User-Agent": "PostmanRuntime/7.42.0",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Iid": bot_id
    }

    date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    body = {
        "filter": {
            "dateFilter": {
                "date": date,
                "timeZoneOffSet": 0
            }
        }
    }
    
    csv_filename = f"Agent_Status_Details_{date}.csv"
    offset = 0
    processed_user_ids = set()
    all_data = []

    while True:
        response = requests.post(url.format(offset=offset), headers=headers, json=body)
        response.raise_for_status()
        response_data = response.json()
        
        if not response_data.get("data"):
            break

        all_data.extend(process_response(response_data, processed_user_ids))
        offset += response_data.get("limit", 50)

        if not response_data.get("hasMore", False):
            break

    save_to_excel(all_data, csv_filename)

if __name__ == "__main__":
    get_agent_status_details()
