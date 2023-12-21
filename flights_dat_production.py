from datetime import datetime, timedelta
from operator import index
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

from FlightRadar24 import FlightRadar24API
import pandas as pd

default_args = {
    "owner": "matsouto",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 20),
    "email": ["matsouto55@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "flight_dag",
    default_args=default_args,
    description="Flight data ETL for Embraer aircrafts",
)


def extract(**kwargs):
    # ------- Flightradar API --------
    api = FlightRadar24API()

    # Configuring the api
    flight_tracker = api.get_flight_tracker_config()
    flight_tracker.limit = 1500
    api.set_flight_tracker_config(flight_tracker)

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

    # Appends each flight to a list
    all_flights = []
    for aircraft_type in embraer_arcrafts:
        for flight in flights_dict[aircraft_type]:
            # Updates some informations from the flight
            try:
                flight_details = api.get_flight_details(flight)
                flight.set_flight_details(flight_details)
            except Exception as e:
                pass

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
            all_flights.append(_dict)

    # Generate the CSV file
    flights_df = pd.DataFrame(all_flights)
    kwargs["ti"].xcom_push(key="extracted_data", value=flights_df.to_csv(index=False))

    # ------- Load --------
    # flights_df.to_csv("./data/flights.csv", index=False)


extract = PythonOperator(
    task_id="extraction",
    python_callable=extract,
    dag=dag,
)

load = S3CreateObjectOperator(
    task_id="load",
    aws_conn_id="AWS_CONN",
    s3_bucket="matsouto-flights-elt",
    s3_key=f"{datetime.now().strftime('%Y-%m-%d %H:%M')}.csv",
    data="{{ ti.xcom_pull(key='extracted_data') }}",
    replace=False,
)

extract >> load
