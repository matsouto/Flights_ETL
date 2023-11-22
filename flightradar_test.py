from FlightRadar24 import FlightRadar24API
import pandas as pd
import json
import requests

# Creating the api object
api = FlightRadar24API()

# Configuring the api
flight_tracker = api.get_flight_tracker_config()
flight_tracker.limit = 5
api.set_flight_tracker_config(flight_tracker)

# Getting all the flights
flights = api.get_flights()
flight_details = api.get_flight_details(flights[0])
flights[0].set_flight_details(flight_details)
print(
    flights[0].airline_name,
    " ",
    flights[0].aircraft_model,
    " ",
    flights[0].id,
)

# print(flight.longitude)
# print(len(flights))

# Creating a json from flight data
# for flight in flights:
#   # Setting all avaliable data
#   flight_details = api.get_flight_details(flight=flight)
#   flight.set_flight_details(flight_details=flight_details)

#   _dict = {
#     'id': flight.id,
#     'lat': flight.latitude,
#     'lon': flight.longitude,
#     'heading': flight.heading,
#     'altitude': flight.altitude,
#     'gspeed': flight.ground_speed,
#     'time': flight.time,
#     'model': flight.aircraft_model,
#     'iairport': flight.origin_airport_name,
#     'fairport': flight.destination_airport_name,
#   }
#     print(flight.latitude)
