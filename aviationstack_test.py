import pandas as pd
import json
import requests
from aux import Flight

# ------- Aviationstack API -------
access_key = "ec56dc99dafcc4fc9ba0b44c98f65b43"
limit = 100  # Maximum number of queries for free account
flight_status = "active"  # squeduled, active, landed, cancelled, incident, diverted

url = f"http://api.aviationstack.com/v1/flights"  # Real-time flights endpoint
params = {"access_key": access_key, "limit": limit, "flight_status": flight_status}
response = requests.get(url, params=params)
response_json = response.json()

data_json = response_json["data"]

# Creating a list of Flight objects
flights_list = []
for flight_json in data_json:
    _dict = {
        "flight_date": flight_json["flight_date"],
        "dtime": flight_json["departure"]["scheduled"],
        "atime": flight_json["arrival"]["scheduled"],
        "diata": flight_json["departure"]["iata"],
        "aiata": flight_json["arrival"]["iata"],
        "airline": flight_json["airline"]["name"],
        "aircraft": flight_json["aircraft"],
    }
    flights_list.append(_dict)

flights_df = pd.DataFrame(flights_list)
flights_df.to_csv("./data/flights.csv", index=False)
