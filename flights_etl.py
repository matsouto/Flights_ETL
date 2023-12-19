from FlightRadar24 import FlightRadar24API
import pandas as pd
import json

# ------- Flightradar API --------
api = FlightRadar24API()

# Configuring the api
flight_tracker = api.get_flight_tracker_config()
flight_tracker.limit = 1500
api.set_flight_tracker_config(flight_tracker)

# ------- Zones --------
# Getting all the zones
zones = api.get_zones()

# Write all the zones possible to a json file
with open("./data/zones.json", "w") as file:
    json_data = json.dumps(zones, indent=4)
    file.write(json_data)


# ------- Flights --------
# Getting all Embraer commercial jets flights
def get_flights(aircraft_type: str):
    # Gets all the flights for a specific aircraft
    return api.get_flights(aircraft_type=aircraft_type)


embraer_arcrafts = [
    "E170",
    "E75L",
    "E75S",
    "E190",
    "E290",
    "E195",
    "E295",
    "E135",
    "E145",
]
# Creating a dictionary for the flights
flights_dict = {}
for aircraft_type in embraer_arcrafts:
    flights = get_flights(aircraft_type)
    flights_dict[aircraft_type] = flights


# Prints the total number of flights
total = 0
for aircraft_type in embraer_arcrafts:
    total = total + len(flights_dict[aircraft_type])
print(f"Total number of flights: {total}", "\n")
print("------------ Start pre-loading process ---------------", "\n")

# Appends each flight to a list
i = 0
all_flights = []
for aircraft_type in embraer_arcrafts:
    for flight in flights_dict[aircraft_type]:
        # Updates some informations from the flight
        try:
            flight_details = api.get_flight_details(flight)
            flight.set_flight_details(flight_details)
        except Exception as e:
            print(f"Error retrieving flight details for flight ID {flight.id}: {e}")

        # Creating the object of the flight
        _dict = {
            "flight_id": flight.id,
            "heading": flight.heading,
            "altitude": flight.altitude,
            "ground_speed": flight.ground_speed,
            "aircraft_code": flight.aircraft_code,
            "origin_airport": flight.origin_airport_iata,
            "destination_airport": flight.destination_airport_iata,
            "airline_iata": flight.airline_iata,
            "onground": flight.on_ground,
            "latitude": flight.latitude,
            "longitude": flight.longitude,
            "aircraft_model": getattr(flight, "aircraft_model", "N/A"),
            "aircraft_code": flight.aircraft_code,
            "aircraft_country_id": getattr(flight, "aircraft_country_id", "N/A"),
            "aircraft_type": aircraft_type,
            "time": flight.time,
        }
        print("Iteration: ", i)
        print("Flight ID: ", _dict["flight_id"])
        print("Aircraft model:", _dict["aircraft_model"])
        print()
        all_flights.append(_dict)
        i = i + 1

# ------- CSV --------
flights_df = pd.DataFrame(all_flights)
flights_df.to_csv("./data/flights.csv", index=False)
