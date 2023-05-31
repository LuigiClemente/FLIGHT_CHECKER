from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import XCom
from airflow.utils.dates import datetime, timedelta
from jenkins_operator import JenkinsJobTriggerOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.jenkins.hooks.jenkins import JenkinsHook
import logging
import os
import requests
import json
import time
import csv
from dotenv import load_dotenv
import pytz  # Import pytz for time zone conversion

load_dotenv()

FLIGHTS_API_KEY = os.getenv("FLIGHTS_API_KEY")
AIRPORTS = os.getenv("AIRPORTS")
AIRLINES = os.getenv("AIRLINES")
DELAY_THRESHOLD = int(os.getenv("DELAY_THRESHOLD", "180"))
TIME_TO_DEPARTURE_THRESHOLD = int(os.getenv("TIME_TO_DEPARTURE_THRESHOLD", "180"))
CANCELLED_FLIGHT_TIME_WINDOW_START = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_START", "60"))
CANCELLED_FLIGHT_TIME_WINDOW_END = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_END", "90"))
API_HOST = os.getenv("API_HOST", "https://airlabs.co/api/v9")
API_ENDPOINT = os.getenv("API_ENDPOINT", "schedules")
IGNORED_DESTINATIONS_BCN = os.getenv("IGNORED_DESTINATIONS_BCN", "").split(",")
IGNORED_DESTINATIONS_AMS = os.getenv("IGNORED_DESTINATIONS_AMS", "").split(",")
JENKINS_URL = os.getenv("JENKINS_URL")
JENKINS_USERNAME = os.getenv("JENKINS_USERNAME")
JENKINS_JOB_NAME = os.getenv("JENKINS_JOB_NAME")
JENKINS_TOKEN = os.getenv("JENKINS_TOKEN")
JENKINS_CONNECTION_ID = os.getenv("JENKINS_CONNECTION_ID")
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "5"))
API_RETRY_DELAY_BASE = int(os.getenv("API_RETRY_DELAY_BASE", "2"))
REQUIRED_ENV_VARIABLES = os.getenv("REQUIRED_ENV_VARIABLES", "").split(",")
CSV_FILE_NAME = os.getenv("CSV_FILE_NAME", "conditions_flights.csv")
API_CALL_TIME_ONGOING_DELAYS = int(os.getenv("API_CALL_TIME_ONGOING_DELAYS", "1"))
API_CALL_TIME_BEFORE_7AM = int(os.getenv("API_CALL_TIME_BEFORE_7AM", "10"))
API_CALL_TIME_DEFAULT = int(os.getenv("API_CALL_TIME_DEFAULT", "3"))
DAG_START_DATE = datetime.strptime(os.getenv("DAG_START_DATE", "2023-05-21"), "%Y-%m-%d")
DAG_RETRIES = int(os.getenv("DAG_RETRIES", "1"))
DAG_RETRY_DELAY = timedelta(minutes=int(os.getenv("DAG_RETRY_DELAY", "1")))
JENKINS_PARAMETERS = json.loads(os.getenv("JENKINS_PARAMETERS", '{"key": "value"}'))
JENKINS_SLEEP_TIME = int(os.getenv("JENKINS_SLEEP_TIME", "30"))
JENKINS_MAX_TRIES = int(os.getenv("JENKINS_MAX_TRIES", "10"))
JENKINS_ALLOWED_STATES = os.getenv("JENKINS_ALLOWED_STATES", "SUCCESS").split(",")


class FlightChecker:
    def __init__(self):
        try:
            self.api_key = FLIGHTS_API_KEY
            self.airports = AIRPORTS
            self.airlines = AIRLINES
            self.delay_threshold = DELAY_THRESHOLD
            self.time_to_departure_threshold = TIME_TO_DEPARTURE_THRESHOLD
            self.cancelled_flight_time_window_start = CANCELLED_FLIGHT_TIME_WINDOW_START
            self.cancelled_flight_time_window_end = CANCELLED_FLIGHT_TIME_WINDOW_END
            self.api_host = API_HOST
            self.api_endpoint = API_ENDPOINT
            self.ignored_destinations_bcn = IGNORED_DESTINATIONS_BCN
            self.ignored_destinations_ams = IGNORED_DESTINATIONS_AMS
            self.last_delay_print_time = {}  # Stores the last delay print time for each airport

            self.validate_environment_variables()
        except Exception as e:
            logging.error(f"Error initializing FlightChecker: {str(e)}")
            raise

    def validate_environment_variables(self):
        """
        Validates the presence and validity of required environment variables.
        Raises a ValueError if any of the required variables are missing or empty.
        """
        required_variables = [
            "FLIGHTS_API_KEY", "AIRPORTS", "AIRLINES", "DELAY_THRESHOLD",
            "TIME_TO_DEPARTURE_THRESHOLD", "CANCELLED_FLIGHT_TIME_WINDOW_START",
            "CANCELLED_FLIGHT_TIME_WINDOW_END"
        ]
        for var in required_variables:
            if not os.getenv(var):
                raise ValueError(f"Environment variable {var} is missing or empty")

    def load_flight_data(self, **context):
        """
        Loads flight data from the API and stores it in XCom.
        Retries the API request with exponential backoff in case of failures.
        Args:
            context (dict): The task context dictionary
        """
        try:
            url = f"{self.api_host}/{self.api_endpoint}?dep_iata={self.airports}&api_key={self.api_key}"
            retry_count = 0
            max_retries = API_MAX_RETRIES
            while retry_count < max_retries:
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    flight_data = response.json()
                    # Store flight data in XCom
                    context['ti'].xcom_push(key='flight_data', value=flight_data)
                    break
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to load flight data: {str(e)}")
                    logging.info(f"Retrying in {API_RETRY_DELAY_BASE ** retry_count} seconds...")
                    time.sleep(API_RETRY_DELAY_BASE ** retry_count)
                    retry_count += 1
            else:
                raise RuntimeError("Failed to load flight data after multiple retries")
        except Exception as e:
            logging.error(f"Error loading flight data: {str(e)}")
            raise

    def analyze_delays(self, **context):
        """
        Analyzes flight delays for each airport and performs appropriate actions.
        Args:
            context (dict): The task context dictionary
        """
        try:
            # Retrieve flight data from XCom
            flight_data = context['ti'].xcom_pull(key='flight_data')
            if flight_data is None:
                logging.warning("Flight data is not loaded")
                return

            ongoing_delays = False
            flights_list = []

            for flight in flight_data:
                airport = flight[0]
                if airport == "BCN":
                    ignored_destinations = self.ignored_destinations_bcn
                elif airport == "AMS":
                    ignored_destinations = self.ignored_destinations_ams
                else:
                    continue

                if flight[1] in self.airlines.split(",") and flight[7] not in ignored_destinations:
                    dep_time = datetime.strptime(flight[2], "%Y-%m-%d %H:%M")
                    dep_time = pytz.utc.localize(dep_time)  # Convert departure time to UTC
                    dep_delayed = int(flight[3])
                    status = flight[4]

                    if dep_delayed > self.delay_threshold and dep_time > datetime.now(pytz.utc) + timedelta(
                        minutes=self.time_to_departure_threshold):
                        ongoing_delays = True

                        flight_iata = flight[5]
                        if airport in self.last_delay_print_time and flight_iata in self.last_delay_print_time[airport]:
                            continue  # Skip already processed delays

                        logging.info(f"Flight {flight_iata} is delayed for airport {airport}.")
                        self.notify_plugin("Delayed", flight, airport=airport, flight_iata=flight_iata)

                        # Update last delay print time
                        if airport in self.last_delay_print_time:
                            self.last_delay_print_time[airport].append(flight_iata)
                        else:
                            self.last_delay_print_time[airport] = [flight_iata]

                        # Only acknowledge a cancelled flight if a delay has been printed for the same airport
                        if status == "cancelled":
                            time_since_last_delay = (
                                datetime.now(pytz.utc) - self.last_delay_print_time[airport][-1]).total_seconds() / 60
                            if self.cancelled_flight_time_window_start < time_since_last_delay < self.cancelled_flight_time_window_end:
                                logging.info(f"Flight {flight_iata} is cancelled for airport {airport}.")
                                self.notify_plugin("Cancelled", flight, airport=airport, flight_iata=flight_iata)

                        # Add flight to the list for CSV creation
                        flights_list.append({
                            'Flight Number': flight[5],
                            'Departure Airport': flight[0],
                            'Departure Time': flight[2],
                            'Delay (Minutes)': flight[3],
                            'Status': flight[4],
                            'Airline': flight[1],
                            'Destination': flight[7]
                        })

            context['ti'].xcom_push(key='ongoing_delays', value=ongoing_delays)
            context['ti'].xcom_push(key='flights_list', value=flights_list)

        except Exception as e:
            logging.error(f"Error analyzing delays: {str(e)}")
            raise

    def create_csv_file(self, **context):
        """
        Creates a CSV file with flights that meet the specified conditions.
        Args:
            context (dict): The task context dictionary
        """
        try:
            flights_list = context['ti'].xcom_pull(key='flights_list')
            if flights_list is None:
                logging.warning("Flights list is empty")
                return

            filename = CSV_FILE_NAME
            headers = [
                'Flight Number',
                'Departure Airport',
                'Departure Time',
                'Delay (Minutes)',
                'Status',
                'Airline',
                'Destination'
            ]

            with open(filename, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=headers)
                writer.writeheader()
                writer.writerows(flights_list)

            logging.info(f"CSV file '{filename}' created with flights that meet the specified conditions.")
        except Exception as e:
            logging.error(f"Error creating CSV file: {str(e)}")
            raise

    def get_next_call_time(self, ongoing_delays, current_time):
        """
        Determine the next API call time based on the current state of delays and time of day.
        Args:
            ongoing_delays (bool): If there are any ongoing flight delays
            current_time (datetime): Current datetime
        Returns:
            next_call_time (datetime): The next API call time
        """
        if ongoing_delays:
            # If there are ongoing delays, check every hour
            next_call_time = current_time + timedelta(hours=API_CALL_TIME_ONGOING_DELAYS)
        elif current_time.hour < 7:
            # If it's before 7 AM, check at 10 AM
            next_call_time = current_time.replace(hour=10, minute=0, second=0)
        else:
            # Otherwise, check every 3 hours
            next_call_time = current_time + timedelta(hours=API_CALL_TIME_DEFAULT)

        return next_call_time

    def notify_plugin(self, status, flight_info, airport=None, flight_iata=None):
        """
        Notifies a plugin or external service about a flight status change.
        Customize this method to implement the desired notification functionality.
        Args:
            status (str): The flight status (e.g., "Delayed", "Cancelled")
            flight_info (list): Information about the flight
            airport (str): The airport code
            flight_iata (str): The flight IATA code
        """
        # Example implementation: Log the notification details
        logging.info(
            f"Notifying plugin: Status='{status}', Flight Info='{flight_info}', Airport='{airport}', Flight IATA='{flight_iata}'")


class DelayedApiCallSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DelayedApiCallSensor, self).__init__(*args, **kwargs)
        self.last_api_call_time = None

    def poke(self, context):
        flight_checker = FlightChecker()
        ongoing_delays = context['ti'].xcom_pull(task_ids='analyze_delays_task', key='ongoing_delays')

        current_time = datetime.now()

        next_call_time = flight_checker.get_next_call_time(ongoing_delays, current_time)

        if self.last_api_call_time is None or self.last_api_call_time < next_call_time:
            if current_time >= next_call_time:
                self.last_api_call_time = current_time  # Record the last successful API call time
                return True
            else:
                return False
        else:
            return False


default_args = {
    'start_date': DAG_START_DATE,
    'retries': DAG_RETRIES,
    'retry_delay': DAG_RETRY_DELAY
}

with DAG(
        'flight_checker',
        default_args=default_args,
        description='Flight Checker DAG',
        schedule_interval=timedelta(minutes=1),
        catchup=False
) as dag:
    flight_checker = FlightChecker()

    delayed_api_call_sensor = DelayedApiCallSensor(
        task_id='delayed_api_call_sensor',
        dag=dag,
    )

    load_flight_data_task = PythonOperator(
        task_id='load_flight_data_task',
        python_callable=flight_checker.load_flight_data,
        provide_context=True,
        dag=dag,
    )

    analyze_delays_task = PythonOperator(
        task_id='analyze_delays_task',
        python_callable=flight_checker.analyze_delays,
        provide_context=True,
        dag=dag,
    )

    create_csv_file_task = PythonOperator(
        task_id='create_csv_file_task',
        python_callable=flight_checker.create_csv_file,
        provide_context=True,
        dag=dag,
    )

    jenkins_trigger = JenkinsJobTriggerOperator(
        task_id='trigger_jenkins_job',
        job_name=JENKINS_JOB_NAME,
        parameters=JENKINS_PARAMETERS,
        jenkins_connection_id=JENKINS_CONNECTION_ID,
        sleep_time=JENKINS_SLEEP_TIME,
        max_try_before_job_appears=JENKINS_MAX_TRIES,
        allowed_jenkins_states=JENKINS_ALLOWED_STATES,
        dag=dag,
    )

    delayed_api_call_sensor >> load_flight_data_task >> analyze_delays_task >> create_csv_file_task >> jenkins_trigger
