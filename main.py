import requests
import json

#Load credentials
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
    print("Theres missings fields in the json")
    exit(1)

url = f"{host}/agentassist/api/public/analytics/account/{account_id}/agentstatusdetails"


query_params = {
    "limit": 8,
    "offset": 0
}


headers = {
    "Content-Type": "application/json",
    "auth": jwt_token,
    "Content-Type": "application/json",
    "User-Agent": "PostmanRuntime/7.42.0",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive"
}


body = {
    "filter": {
        "dateFilter": {
            "date": "2024-11-18",  
            "timeZoneOffSet": -330  
        }
    }
}

try:
    response = requests.post(url, headers=headers, params=query_params, json=body)

    if response.status_code == 200:
        print("Respuesta exitosa:")
        print(response.json())  
    else:
        print(f"Error en la solicitud: {response.status_code}")
        print(response.text)  

except requests.RequestException as e:
    print(f"Error al realizar la solicitud: {e}")
