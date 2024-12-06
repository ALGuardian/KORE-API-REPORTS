import pandas as pd
from datetime import datetime, timedelta
import requests
import json
import time 
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os


YESTERDAY_STRING = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
YESTERDAY = datetime.now() - timedelta(days=1)

CREDENTIALS = "Credentials/kore_credentials.json"

CALL_DETAILS_OUTPUT_PATH = 'Data/CallsDetails.json'


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

def create_save_data_path(Path):
    """
    Ensures that the directory for a given file path exists. If the directory does not exist, 
    it creates all necessary intermediate directories.

    Parameters:
    - Path (str): The full path (including file name) where data will be saved.
    """

    output_directory = os.path.dirname(Path)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

def fetch_kore_call_details():
    """
    Fetches conversation call details from Kore.ai's API in a paginated manner,
    accumulates the data, and saves it into a single JSON file. A delay is added
    between each request to avoid overloading the API.
    """

    with open(CREDENTIALS, 'r') as file:
        credentials = json.load(file)

    host = credentials["host"]
    account_id = credentials["account_id"]
    jwt_token = credentials["jwt_token"]
    bot_id = credentials['app_id']

    headers = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9",
        "accountid": account_id,
        "app-language": "en",
        "content-type": "application/json;charset=UTF-8",
        "referer": "smartassist",
        "auth": jwt_token,
        "iid": bot_id,
        "User-Agent": "PostmanRuntime/7.42.0",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }

    yesterday = YESTERDAY
    start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

    payload = {
        "startDate": start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "endDate": end_date.strftime("%Y-%m-%d %H:%M:%S"),
        "timeZoneOffset": 0
    }

    print( f"Fetching data for Start Date: {start_date.strftime('%Y-%m-%d %H:%M:%S')}, End Date: {end_date.strftime('%Y-%m-%d %H:%M:%S')}")

    all_data = []
    offset = 0
    request_count = 0

    while True:
        url = f'https://{host}/agentassist/api/public/analytics/account/{account_id}/v2/calldetails?offset={offset}'
        try:
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting for 60 seconds...")
                time.sleep(60)
                continue
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error during request: {e}")
            break

        data = response.json()
        num_results = data.get("numResults", 0)

        if "data" in data:
            # Agrega toda la data sin filtrar
            all_data.extend(data["data"])
            print(f"Added {len(data['data'])} records. Total so far: {len(all_data)}.")
        else:
            print("No 'data' field in response or no more records.")
            break

        if num_results == 0 or len(data.get("data", [])) == 0:
            break

        offset += 100
        request_count += 1

        if request_count >= 60:
            print("Reached 60 API calls. Pausing for 60 seconds...")
            time.sleep(60)
            request_count = 0

    # Guardar los datos en un archivo JSON
    if all_data:
        df = pd.DataFrame(all_data)

        create_save_data_path(CALL_DETAILS_OUTPUT_PATH)

        # Guardar como JSON
        try:
            df.to_json(CALL_DETAILS_OUTPUT_PATH, orient='records', indent=4)
            print(f"JSON file saved at: {CALL_DETAILS_OUTPUT_PATH}")
        except Exception as e:
            print(f"Error saving JSON: {e}")
    else:
        print("No data found to save.")

def read_json_to_dataframe(json_path):
    try:
        with open(json_path, 'r') as file:
            data = json.load(file)
        # Ensure the data is a list
        if not isinstance(data, list):
            data = [data]
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        raise

def upload_to_snowflake(df, table_name):
    conn = connect_to_snowflake()
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name,
            database="ICON_GLG",
            schema="KORE",
            quote_identifiers=False
        )
        print(f"Upload successful: {success}, Chunks: {nchunks}, Rows: {nrows}")
    except Exception as e:
        print(f"Error uploading DataFrame to Snowflake: {e}")
    finally:
        conn.close()

if __name__ == "__main__":

    fetch_kore_call_details()
    
    # Convert JSON to DataFrame
    df = read_json_to_dataframe(CALL_DETAILS_OUTPUT_PATH)

    # Ensure the DataFrame columns match the Snowflake table schema
    df = df.rename(columns={
        "sessionId": "SESSIONID",
        "channel": "CHANNEL",
        "sessionStartTime": "SESSIONSTARTTIME",
        "sessionEndTime": "SESSIONENDTIME",
        "botId": "BOTID",
        "userId": "USERID",
        "channelSpecificUserId": "CHANNELSPECIFICUSERID",
        "orgId": "ORGID",
        "smartStatus": "SMARTSTATUS",
        "reason": "REASON",
        "finalStatus": "FINALSTATUS",
        "conversationId": "CONVERSATIONID",
        "isVoicemail": "ISVOICEMAIL",
        "Direction": "DIRECTION",
        "Reason": "REASON_2",
        "dispositions": "DISPOSITIONS",
        "dispositionRemarks": "DISPOSITIONREMARKS",
        "metaInfo": "METAINFO",
        "destinations": "DESTINATIONS",
        "csatScore": "CSATSCORE"
    })

    # Upload DataFrame to Snowflake
    upload_to_snowflake(df, table_name="AGENT_SESSION_DATA")


