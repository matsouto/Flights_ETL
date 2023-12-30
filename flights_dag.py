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

# TODO: Adicionar vertical_speed, callsign, aircraft_age, status_text
# TODO: Maybe get data from the airport directelly from the API instead of info tables

default_args = {
    "owner": "matsouto",
    "depends_on_past": False,
    "start_date": datetime(2023, 12, 21),
    # "end_date": datetime(2024, 1, 1),
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
            "aircraft_registration": flight.registration,
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
    # Prevents in case of duplicated flight records
    flights_df.drop_duplicates(subset=["flight_id"], inplace=True)
    # Removes the aircrafts without registration
    flights_df.dropna(subset=["aircraft_registration"], inplace=True)
    flights_df.reset_index(drop=True, inplace=True)

    # --------- AIRLINES DIMENSION TABLE ----------

    # Remove null values for airline_iata
    flights_df_airlines = flights_df.copy()
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

    # Create airlines_dim DataFrame
    airlines_dim = pd.DataFrame()
    airlines_dim["airline_name"] = _merged_df["name"]
    airlines_dim["airline_iata"] = _merged_df["airline_iata"]
    airlines_dim.drop_duplicates(subset="airline_iata", inplace=True)
    airlines_dim.reset_index(drop=True, inplace=True)
    airlines_dim["id"] = airlines_dim.index

    # Replace empty strings with pd.NA
    airlines_dim["airline_name"].replace("", pd.NA, inplace=True)

    # --------- AIRCRAFTS DIMENSION TABLE ----------
    # Create aircrafts_dim DataFrame
    aircrafts_dim = pd.DataFrame()
    aircrafts_dim["id"] = flights_df.index
    aircrafts_dim["aircraft_registration"] = flights_df["aircraft_registration"]
    aircrafts_dim["aircraft_model"] = flights_df["aircraft_model"]
    aircrafts_dim["aircraft_code"] = flights_df["aircraft_code"]
    _merged_df = flights_df.merge(
        airlines_dim, left_on="airline_iata", right_on="airline_iata", how="left"
    )
    aircrafts_dim["airline_id"] = (
        _merged_df["id"].fillna(-1).astype(int).replace(-1, pd.NA)
    )

    # --------- AIRPORTS DIMENSION TABLE ----------
    # Remove null values for airline_iata
    flights_df_airports = flights_df.copy()
    flights_df_airports.dropna(subset=["origin_airport"], inplace=True)
    flights_df_airports_destination = flights_df.copy()
    flights_df_airports_destination.dropna(subset=["destination_airport"], inplace=True)

    airports_data_df = pd.read_csv("./data/airports.csv", sep=",")
    airports_data_df["airport_latitude"] = airports_data_df["latitude"]
    airports_data_df["airport_longitude"] = airports_data_df["longitude"]

    # Helper function to create the airport DataFrames
    def create_airports_df(
        flights_df_airports, airports_data_df, subset: str
    ) -> pd.DataFrame:
        # Merge the DataFrames based on 'iata_code'
        _merged_df = pd.merge(
            flights_df_airports,
            airports_data_df,
            left_on=subset,
            right_on="iata",
            how="left",
        )

        # Create airports DataFrame
        airports_df = pd.DataFrame()
        airports_df["airport_name"] = _merged_df["airport"]
        airports_df["airport_iata"] = _merged_df["iata"]
        airports_df["airport_icao"] = _merged_df["icao"]
        airports_df["airport_latitude"] = _merged_df["airport_latitude"]
        airports_df["airport_longitude"] = _merged_df["airport_longitude"]
        airports_df["airport_region_name"] = _merged_df["region_name"]
        airports_df["airport_country_code"] = _merged_df["country_code"]

        airports_df.dropna(inplace=True)
        airports_df.drop_duplicates(subset="airport_iata", inplace=True)

        return airports_df

    # Create the DataFrame with the origin and destination airports
    airports_dim_origin = create_airports_df(
        flights_df_airports, airports_data_df, "origin_airport"
    )
    airports_dim_destination = create_airports_df(
        flights_df_airports, airports_data_df, "destination_airport"
    )

    # Concatenate both DataFrames and remove duplicates
    airports_dim = pd.concat([airports_dim_origin, airports_dim_destination], axis=0)
    airports_dim.drop_duplicates(subset="airport_iata", inplace=True)
    airports_dim.reset_index(inplace=True, drop=True)
    airports_dim["id"] = airports_dim.index

    # --------- TIME DIMENSION TABLE ----------

    # The time comes in the UTC format from the API, thus, it have to be converted
    _datetime_objects = [
        datetime.utcfromtimestamp(timestamp) for timestamp in flights_df["time"].values
    ]

    # Create the time dimension DataFrame
    datetimes_dim = pd.DataFrame()
    _years = [dt.year for dt in _datetime_objects]
    _months = [dt.month for dt in _datetime_objects]
    _days = [dt.day for dt in _datetime_objects]
    _hours = [dt.hour for dt in _datetime_objects]
    _minutes = [dt.minute for dt in _datetime_objects]
    _seconds = [dt.second for dt in _datetime_objects]

    datetimes_dim["utc"] = flights_df["time"]
    datetimes_dim["year"] = _years
    datetimes_dim["month"] = _months
    datetimes_dim["day"] = _days
    datetimes_dim["hour"] = _hours
    datetimes_dim["minute"] = _minutes
    datetimes_dim["second"] = _seconds

    datetimes_dim.drop_duplicates(subset="utc", inplace=True)
    datetimes_dim.reset_index(inplace=True, drop=True)
    datetimes_dim["id"] = datetimes_dim.index

    # --------- FLIGHTS FACTS TABLE ----------

    flights_fact = pd.DataFrame()
    flights_fact["heading"] = flights_df["heading"]
    flights_fact["altitude"] = flights_df["altitude"]
    flights_fact["ground_speed"] = flights_df["ground_speed"]
    flights_fact["onground"] = flights_df["onground"]
    flights_fact["latitude"] = flights_df["latitude"]
    flights_fact["longitude"] = flights_df["longitude"]
    flights_fact["longitude"] = flights_df["longitude"]

    # Creating the foreign keys
    _merged_df = flights_df.merge(
        airports_dim, left_on="origin_airport", right_on="airport_iata", how="left"
    )
    flights_fact["origin_airport_id"] = (
        _merged_df["id"].fillna(-1).astype(int).replace(-1, pd.NA)
    )

    _merged_df = flights_df.merge(
        airports_dim, left_on="destination_airport", right_on="airport_iata", how="left"
    )
    flights_fact["destination_airport_id"] = (
        _merged_df["id"].fillna(-1).astype(int).replace(-1, pd.NA)
    )

    _merged_df = flights_df.merge(
        aircrafts_dim,
        left_on="aircraft_registration",
        right_on="aircraft_registration",
        how="left",
    )
    flights_fact["aircraft_id"] = (
        _merged_df["id"].fillna(-1).astype(int).replace(-1, pd.NA)
    )

    _merged_df = flights_df.merge(
        datetimes_dim, left_on="time", right_on="utc", how="left"
    )
    flights_fact["datetimes_id"] = (
        _merged_df["id"].fillna(-1).astype(int).replace(-1, pd.NA)
    )

    # Push data to Airflow XCOM
    kwargs["ti"].xcom_push(key="flights_fact", value=flights_fact)
    kwargs["ti"].xcom_push(key="airports_dim", value=airports_dim)
    kwargs["ti"].xcom_push(key="datetimes_dim", value=datetimes_dim)
    kwargs["ti"].xcom_push(key="airlines_dim", value=airlines_dim)
    kwargs["ti"].xcom_push(key="aircrafts_dim", value=aircrafts_dim)


def load_to_s3(**kwargs):
    """
    TODO: Send the flights_df in a "raw-data" folder and the data model
    as a .json in a "transformed-data" folder
    """
    # Retrieve AWS credentials from Airflow Connection
    logging.info("Retrieving credentials")
    aws_conn_id = "AWS_CONN"
    aws_hook = AwsBaseHook(aws_conn_id)
    credentials = aws_hook.get_credentials()

    logging.info("Starting connection")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
    )

    table_list = [
        "flights_facts",
        "airports_dim",
        "datetimes_dim",
        "airlines_dim",
        "aircrafts_dim",
    ]
    logging.info("Starting data upload")
    # Upload the files
    upload_datetime = datetime.now().strftime("%Y-%m-%d %H:%M")
    for table in table_list:
        # Pull data from Airflow XCOM
        data = kwargs["ti"].xcom_pull(key=table)
        df = pd.DataFrame.from_dict(data)
        # Convert to csv file
        csv_data = df.to_csv(index=False)

        # Load data to S3
        s3.put_object(
            Body=csv_data,
            Bucket="matsouto-flights-elt",
            Key=f"transformed_data/{upload_datetime}/{table}.csv",
        )
        logging.info(f"{table} uploaded to S3")


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
