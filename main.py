import requests
import json
import csv

# Load credentials
credentials_path = "Credentials/kore_credentials.json"

try:
    with open(credentials_path, "r") as file:
        credentials = json.load(file)
except FileNotFoundError:
    print(f"The file was not found: {credentials_path}")
    exit(1)
except json.JSONDecodeError:
    print(f"Error with the json: {credentials_path}")
    exit(1)

host = credentials.get("host")
account_id = credentials.get("account_id")
jwt_token = credentials.get("jwt_token")

if not all([host, account_id, jwt_token]):
    print("There's missing fields in the json")
    exit(1)

# Fixed date: 2024-11-19
formatted_date = "2024-11-19"

url = f"https://{host}/agentassist/api/public/analytics/account/{account_id}/agentstatusdetails"

query_params = {
    "limit": 8,
    "offset": 0
}

headers = {
    "Content-Type": "application/json",
    "auth": jwt_token,
    "User-Agent": "PostmanRuntime/7.42.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}

body = {
    "filter": {
        "dateFilter": {
            "date": formatted_date,
            "timeZoneOffSet": -330
        }
    }
}

try:
    response = requests.post(url, headers=headers, params=query_params, json=body)

    if response.status_code == 200:
        print("Respuesta exitosa. Guardando en CSV...")
        response_data = response.json()

        # Define CSV file name
        csv_filename = f"Call_Details_{formatted_date}.csv"

        # Open CSV file for writing
        with open(csv_filename, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)

            # Write header (keys from the first item in the data list)
            if "data" in response_data and isinstance(response_data["data"], list):
                if response_data["data"]:
                    writer.writerow(response_data["data"][0].keys())

                    # Write each row of data
                    for row in response_data["data"]:
                        writer.writerow(row.values())
                else:
                    print("No hay datos en la respuesta.")
            else:
                print("Formato de respuesta inesperado.")

        print(f"Archivo CSV guardado como {csv_filename}")
    else:
        print(f"Error en la solicitud: {response.status_code}")
        print(response.text)

except requests.RequestException as e:
    print(f"Error al realizar la solicitud: {e}")
