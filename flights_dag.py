from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from FlightRadar24 import FlightRadar24API
import pandas as pd
import logging
import boto3

default_args = {
    "owner": "matsouto",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 21),
    "end_date": datetime(2024, 1, 1),
    "email": ["matsouto55@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "flight_dag",
    default_args=default_args,
    description="Flight data ETL for Embraer aircrafts",
    # schedule_interval="*/30 * * * *",  # Run every 10 minutes
)


def extract_flight_data(**kwargs):
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

    embraer_aircrafts = [
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
    logging.info("Starting extraction")
    flights_dict = {}
    for aircraft_type in embraer_aircrafts:
        flights = get_flights(aircraft_type)
        flights_dict[aircraft_type] = flights
    logging.info("Completed extraction")

    # Counts the total number of flights for reference
    total = 0
    for aircraft_type in embraer_aircrafts:
        total = total + len(flights_dict[aircraft_type])

    # Appends each flight to a list
    all_flights = []
    i = 0
    logging.info("Starting adjustments")
    for aircraft_type in embraer_aircrafts:
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
            i = i + 1
            logging.info(f"Finished {i}/{total}")

    logging.info("Completed adjustments")

    # Generate the CSV file
    flights_df = pd.DataFrame(all_flights)

    # Replace 'N/A' with NaN
    # This is important to prevent errors in the serialization of XCOM
    flights_df.replace("N/A", pd.NA, inplace=True)

    # Push data to Airflow XCOM
    kwargs["ti"].xcom_push(key="extracted_data", value=flights_df)


def load_to_s3(**kwargs):
    # Pull data from Airflow XCOM
    data = kwargs["ti"].xcom_pull(key="extracted_data")
    dataframe = pd.DataFrame.from_dict(data)

    # Convert to csv file
    csv_data = dataframe.to_csv(index=False)

    # Retrieve AWS credentials from Airflow Connection
    aws_conn_id = "AWS_CONN"
    aws_hook = AwsBaseHook(aws_conn_id)
    credentials = aws_hook.get_credentials()

    # Upload the file
    s3 = boto3.client(
        "s3",
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
    )
    s3.put_object(
        Body=csv_data,
        Bucket="matsouto-flights-elt",
        Key=f"{datetime.now().strftime('%Y-%m-%d %H:%M')}.csv",
    )
    logging.info("File uploaded to S3")


extract_task = PythonOperator(
    task_id="extraction",
    python_callable=extract_flight_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load_to_s3,
    dag=dag,
)

extract_task >> load_task
