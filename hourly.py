from datetime import datetime
import pandas as pd
import requests
import time
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
        print(f"The file was not found: {CREDENTIALS}")
        exit(1)
    except json.JSONDecodeError:
        print(f"Error with the JSON: {CREDENTIALS}")
        exit(1)

    host = credentials.get("host")
    account_id = credentials.get("account_id")
    token = credentials.get("jwt_token")
    bot_id = credentials.get("app_id")

    if not all([host, account_id, token]):
        print("Missing fields in the credentials JSON")
        exit(1)

    return host, account_id, token, bot_id

def flatten_data(user_data):
    flattened_rows = []

    for user in user_data:
        user_id = user.get("userId")
        first_name = user.get("firstName")
        last_name = user.get("lastName")
        email = user.get("email")

        for interval in user.get("timeInterval", []):
            start_time = interval.get("startTime")
            end_time = interval.get("endTime")

            for status in interval.get("status", []):
                metric = status.get("metric")
                value = status.get("value")
                sum_time = status.get("stats", {}).get("sum")

                flattened_rows.append({
                    "User ID": user_id,
                    "First Name": first_name,
                    "Last Name": last_name,
                    "Email": email,
                    "Start Time": start_time,
                    "End Time": end_time,
                    "Metric": metric,
                    "Value": value,
                    "Duration (sec)": sum_time
                })

    return flattened_rows

def upload_to_snowflake(data):
    if data:
        conn = connect_to_snowflake()
        try:
            df = pd.DataFrame(data)

            df.columns = [
                "USER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL",
                "START_TIME", "END_TIME", "METRIC", "VALUE", "DURATION_SEC"
            ]

            cursor = conn.cursor()
            cursor.execute("USE DATABASE ICON_GLG")
            cursor.execute("USE SCHEMA KORE")
            print("Uploading data to Snowflake...")

            # Write DataFrame to Snowflake
            success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="AGENT_STATUS2",
            schema="KORE",
            database="ICON_GLG"
            )
        
            if success:
                print(f"Successfully uploaded {nrows} rows in {nchunks} chunks.")
            else:
                print("Data upload failed.")
        finally:
            conn.close()
    else:
        print("No data to upload.")

def get_users_status(host, account_id, token, bot_id):
    url_template = f"https://{host}/agentassist/api/public/analytics/1.1/account/{account_id}/userstatus"
    headers = {
        "Content-Type": "application/json",
        "auth": token,
        "User-Agent": "PostmanRuntime/7.42.0",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Iid": bot_id
    }

    date = datetime.now().strftime("%Y-%m-%d")
    body = {
        "filter": {
            "dateFilter": {
                "startDate": date,
                "endDate": date,
                "timeZoneOffSet": -330
            }
        },
        "granularity": "PT30M"
    }

    offset = 0
    request_count = 0
    all_data = []
    seen_user_ids = set()

    while True:
        body["offset"] = offset
        try:
            response = requests.post(url_template, headers=headers, json=body)

            if response.status_code == 429:
                print("Rate limit exceeded. Waiting for 60 seconds...")
                time.sleep(60)
                continue

            response.raise_for_status()
            data = response.json()

            if "data" in data and isinstance(data["data"], list):
                for user in data["data"]:
                    user_id = user.get("userId")
                    if user_id in seen_user_ids:
                        print(f"Duplicate User ID found: {user_id}. Stopping process.")
                        upload_to_snowflake(all_data)
                        return

                    seen_user_ids.add(user_id)

                filtered_data = [item for item in data["data"] if "timeInterval" in item and item["timeInterval"]]
                flattened_data = flatten_data(filtered_data)
                all_data.extend(flattened_data)

                print(f"Fetched {len(filtered_data)} records. Total offset: {offset}.")

            if not data.get("hasMore", False):
                break

            offset += data.get("limit", 50)
            request_count += 1

            if request_count >= 60:
                print("Reached 60 API calls. Pausing for 60 seconds...")
                time.sleep(60)
                request_count = 0

        except requests.RequestException as e:
            print(f"Error during request: {e}")
            break

    upload_to_snowflake(all_data)

if __name__ == "__main__":
    host, account_id, token, bot_id = check_credentials()
    get_users_status(host, account_id, token, bot_id)
