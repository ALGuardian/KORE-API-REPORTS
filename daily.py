import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


CREDENTIALS = "Credentials/kore_credentials.json"

def load_snowflake_credentials():
    try:
        with open('Credentials/snowflake_credentials.json', 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Issue with Snowflake credentials: {e}")
        raise

def connect_to_snowflake():
    creds = load_snowflake_credentials()
    try:
        return snowflake.connector.connect(
            user=creds['user'],
            password=creds['password'],
            account=creds['account'],
            warehouse=creds['warehouse'],
            database=creds['database'],
            schema=creds['schema']
        )
    except Exception as e:
        print(f"Cannot connect to Snowflake: {e}")
        raise

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
                    "userId": base_info["userId"],
                    "firstName": base_info["firstName"],
                    "lastName": base_info["lastName"],
                    "email": base_info["email"],
                    "customId": base_info["customId"],
                    "primaryStatus": status.get("primaryStatus"),
                    "secondaryStatus": status.get("secondaryStatus"),
                    "startTime": status.get("startTime"),
                    "endTime": status.get("endTime"),
                    "duration": status.get("duration")
                })
        else:
            # Si no hay estados, registrar el usuario sin subestados
            flattened_data.append({
                **base_info,
                "primaryStatus": None,
                "secondaryStatus": None,
                "startTime": None,
                "endTime": None,
                "duration": None
            })
    return flattened_data

def upload_to_snowflake(data):
    conn = connect_to_snowflake()
    try:
        # Convertir la lista de datos en un DataFrame
        df = pd.DataFrame(data)

        df.columns = [col.upper() for col in df.columns]
        
        # Subir el DataFrame a Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="AGENT_STATUS",
            schema="KORE",
            database="ICON_GLG"
        )
        
        if success:
            print(f"Data uploaded successfully: {nrows} rows inserted in {nchunks} chunks.")
        else:
            print("Failed to upload data to Snowflake.")
    except Exception as e:
        print(f"Error uploading DataFrame to Snowflake: {e}")
    finally:
        conn.close()


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
        print(offset)

        if not response_data.get("hasMore", False):
            break

    if all_data:
        upload_to_snowflake(all_data)

if __name__ == "__main__":
    get_agent_status_details()
