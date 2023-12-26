from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

from FlightRadar24 import FlightRadar24API
import pandas as pd
import logging
import boto3
import time
from concurrent.futures import ThreadPoolExecutor

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
    # schedule_interval="*/30 * * * *",  # Run every 30 minutes
)


def extract_flight_data(**kwargs):
    start_time = time.time()

    api = FlightRadar24API()

    # Configuring the API
    flight_tracker = api.get_flight_tracker_config()
    flight_tracker.limit = 1500
    api.set_flight_tracker_config(flight_tracker)

    setup_time = time.time() - start_time
    logging.info(f"Setup time: {setup_time} seconds")

    # Getting all Embraer commercial jets flights
    def get_flights(aircraft_type: str):
        # Gets all the flights for a specific aircraft
        return api.get_flights(aircraft_type=aircraft_type, details=False)

    # API call to get more informations about the flights
    def fetch_flight_details(flight, all_flights: list):
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

    extraction_start_time = time.time()

    # Creating a dictionary for the flights
    flights_dict = {}
    # Counts the total number of flights for reference
    total = 0
    for aircraft_type in embraer_aircrafts:
        flights = get_flights(aircraft_type)
        flights_dict[aircraft_type] = flights
        total = total + len(flights_dict[aircraft_type])

    extraction_time = time.time() - extraction_start_time
    logging.info(f"Extraction time: {extraction_time} seconds")

    # Appends each flight to a list
    all_flights = []
    logging.info(f"Starting details fetching")
    details_start_time = time.time()

    # Parallel processing for the API fligh_details call
    # This part was taking too much time to execute before the fix
    with ThreadPoolExecutor() as executor:
        futures = []
        for aircraft_type in embraer_aircrafts:
            for flight in flights_dict[aircraft_type]:
                # Submit each task to the ThreadPoolExecutor
                futures.append(
                    executor.submit(fetch_flight_details, flight, all_flights)
                )

        # Wait for all tasks to complete
        for future in futures:
            future.result()

    details_time = time.time() - details_start_time
    logging.info(f"Details time: {details_time} seconds")

    # Generate the CSV file
    dataframe_start_time = time.time()
    flights_df = pd.DataFrame(all_flights)

    dataframe_time = time.time() - dataframe_start_time
    logging.info(f"DataFrame creation time: {dataframe_time} seconds")

    # Replace 'N/A' with NaN
    # This is important to prevent errors in the serialization of XCOM
    flights_df.replace("N/A", pd.NA, inplace=True)

    total_time = time.time() - start_time
    logging.info(f"Extraction finished!")
    logging.info(f"Total execution time: {total_time} seconds")

    # Push data to Airflow XCOM
    kwargs["ti"].xcom_push(key="extracted_data", value=flights_df)


def transform_flight_data(**kwargs):
    # Pull data from Airflow XCOM
    data = kwargs["ti"].xcom_pull(key="extracted_data")
    df = pd.DataFrame.from_dict(data)
    flights_df = df.copy()
    flights_df.drop_duplicates(subset=["flight_id"], inplace=True)

    # --------- AIRLINES DIMENSION TABLE ----------

    # Remove null values for airline_iata
    flights_df_airlines = flights_df.copy()
    flights_df_airlines.dropna(subset=["airline_iata"], inplace=True)
    flights_df_airlines.dropna(subset=["airline_iata"], inplace=True)

    airline_data_df = pd.read_csv("./data/iata_airlines.csv", sep=",")

    # Merge the DataFrames based on 'iata_code'
    _merged_df = pd.merge(
        flights_df_airlines,
        airline_data_df,
        left_on="airline_iata",
        right_on="iata_code",
        how="left",
    )

    # Create airlines_dim DataFrame and keep the index from flights_df
    airlines_dim = pd.DataFrame()
    airlines_dim["id"] = _merged_df.index
    airlines_dim["airline_name"] = _merged_df["name"]
    airlines_dim["airline_iata"] = _merged_df["airline_iata"]

    # Replace empty strings with pd.NA
    airlines_dim["airline_name"].replace("", pd.NA, inplace=True)

    # --------- AIRCRAFTS DIMENSION TABLE ----------
    aircrafs_dim = pd.DataFrame()

    # Push data to Airflow XCOM
    kwargs["ti"].xcom_push(key="transformed_data", value=flights_df)


def load_to_s3(**kwargs):
    """
    TODO: Send the flights_df in a "raw-data" folder and the data model
    as a .json in a "transformed-data" folder
    """
    # Pull data from Airflow XCOM
    data = kwargs["ti"].xcom_pull(key="transformed_data")
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

transform_task = PythonOperator(
    task_id="transformation",
    python_callable=transform_flight_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="loading",
    python_callable=load_to_s3,
    dag=dag,
)

extract_task >> transform_task >> load_task
