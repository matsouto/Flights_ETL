import pandas as pd
import json
import requests

access_key = "ec56dc99dafcc4fc9ba0b44c98f65b43"
limit = 1  # Number of rows from query
flight_status = "active"  # squeduled, active, landed, cancelled, incident, diverted

url = f"http://api.aviationstack.com/v1/flights"  # Real-time flights endpoint
params = {"access_key": access_key, "limit": limit, "flight_status": flight_status}
response = requests.get(url, params=params)
response_json = response.json()

with open("test.json", "w") as file:
    json.dump(response_json, file)

# for flight in response_json["data"]:
#     if flight["live"] is False:
#         print(
#             "%s flight %s from %s (%s) to %s (%s) is in the air."
#             % (
#                 flight["airline"]["name"],
#                 flight["flight"]["iata"],
#                 flight["departure"]["airport"],
#                 flight["departure"]["iata"],
#                 flight["arrival"]["airport"],
#                 flight["arrival"]["iata"],
#             )
#         )
