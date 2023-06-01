import logging
import os
import requests
import json
import time
import csv
import pytz
import json


from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator
from airflow.utils.dates import timedelta
from airflow.utils.decorators import apply_defaults

load_dotenv()

def validate_environment_variables():
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

class DelayManager:
    def __init__(self):
        self.call_immediately = False

    def reset(self):
        """
        Reset the delay manager and force an immediate API call.
        """
        self.call_immediately = True

    def get_next_call_time(self, ongoing_delays, current_time):
        """
        Determine the next API call time based on the current state of delays and time of day.
        Args:
            ongoing_delays (list): List of ongoing flight delays
            current_time (datetime): Current datetime
        Returns:
            next_call_time (datetime): The next API call time
        """
        # If the system was just reset, or the delay list is empty, we call the API immediately
        if self.call_immediately or len(ongoing_delays) == 0:
            self.call_immediately = False
            return current_time

        # Get the delay time threshold from environment variable or set to 1 hour by default
        api_call_time_ongoing_delays = int(os.getenv("API_CALL_TIME_ONGOING_DELAYS", "1"))
        
        if ongoing_delays:
            # If there are ongoing delays, check every hour (or as defined by the threshold)
            next_call_time = current_time + datetime.timedelta(hours=api_call_time_ongoing_delays)
        elif current_time.hour < 7:
            # If it's before 7 AM, check at 10 AM
            next_call_time = current_time.replace(hour=10, minute=0, second=0)
        else:
            # Otherwise, check every 3 hours or as defined by the threshold
            api_call_time_default = int(os.getenv("API_CALL_TIME_DEFAULT", "3"))
            next_call_time = current_time + datetime.timedelta(hours=api_call_time_default)

        return next_call_time

class DelayedApiCallSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DelayedApiCallSensor, self).__init__(*args, **kwargs)
        self.last_api_call_time = None

    def poke(self, context):
        delay_manager = DelayManager()
        ongoing_delays = context['ti'].xcom_pull(task_ids='analyze_delays_task', key='ongoing_delays')

        current_time = datetime.now()

        # If ongoing_delays is None, make it an empty list
        if ongoing_delays is None:
            ongoing_delays = []

        next_call_time = delay_manager.get_next_call_time(ongoing_delays, current_time)

        if self.last_api_call_time is None or self.last_api_call_time < next_call_time:
            if current_time >= next_call_time:
                self.last_api_call_time = current_time  # Record the last successful API call time
                return True
            else:
                return False
        else:
            return False

class FlightChecker:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        try:
            self.api_key = os.getenv("FLIGHTS_API_KEY")
            self.airports = os.getenv("AIRPORTS")
            self.airlines = os.getenv("AIRLINES")
            self.delay_threshold = int(os.getenv("DELAY_THRESHOLD", "180"))
            self.time_to_departure_threshold = int(os.getenv("TIME_TO_DEPARTURE_THRESHOLD", "180"))
            self.cancelled_flight_time_window_start = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_START", "60"))
            self.cancelled_flight_time_window_end = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_END", "90"))
            self.api_host = os.getenv("API_HOST", "https://airlabs.co/api/v9")
            self.api_endpoint = os.getenv("API_ENDPOINT", "schedules")
            self.ignored_destinations_bcn = os.getenv("IGNORED_DESTINATIONS_BCN", "").split(",")
            self.ignored_destinations_ams = os.getenv("IGNORED_DESTINATIONS_AMS", "").split(",")
            self.last_delay_print_time = {}  # Stores the last delay print time for each airport

            self.delayed_data = self.load_flight_data()  # Load flight data upon initializing the FlightChecker class

            validate_environment_variables()
        except Exception as e:
            logging.error(f"Error initializing FlightChecker: {str(e)}")
            raise

    def load_flight_data(self):
        try:
            with open("flights.json", 'r') as file:
                data = file.read()
                if not data:
                    self.log.error("flights.json file is empty")
                    return None
                data = json.loads(data)

            def find_delayed_in_dict(obj):
                if "delayed" in obj:
                    return obj["delayed"]
                for k, v in obj.items():
                    if isinstance(v, dict):
                        item = find_delayed_in_dict(v)
                        if item is not None:
                            return item

            delayed_data = find_delayed_in_dict(data)

            if delayed_data is None:
                raise ValueError("No flight delay data in response")

            return delayed_data
        except json.JSONDecodeError as e:
            self.log.error(f"Error decoding JSON: {str(e)}")
        except Exception as e:
            self.log.error(f"Error loading flight data: {str(e)}")

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
                        if airport in self.last_delay_print_time and flight_iata in self.last_delay_print_time[
                            airport]:
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
            if flights_list is None or not flights_list:
                self.log.warning("Flights list is empty")
                return

            filename = os.getenv("CSV_FILE_NAME", "conditions_flights.csv")
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

            self.log.info(f"CSV file '{filename}' created with flights that meet the specified conditions.")
        except Exception as e:
            self.log.error(f"Error creating CSV file: {str(e)}")
            raise

default_args = {
    'start_date': datetime.strptime(os.getenv("DAG_START_DATE", "2023-05-21"), "%Y-%m-%d"),
    'retries': int(os.getenv("DAG_RETRIES", "1")),
    'retry_delay': timedelta(minutes=int(os.getenv("DAG_RETRY_DELAY", "1")))
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
    dag=dag
    )

    jenkins_trigger = JenkinsJobTriggerOperator(
    task_id='trigger_jenkins_job',
    job_name=os.getenv("JENKINS_JOB_NAME"),
    jenkins_connection_id=os.getenv("JENKINS_CONNECTION_ID"),
    parameters=json.loads(os.getenv("JENKINS_PARAMETERS", '{"key": "value"}')),
    sleep_time=int(os.getenv("JENKINS_SLEEP_TIME", "30")),
    dag=dag,
)

delayed_api_call_sensor >> load_flight_data_task >> analyze_delays_task >> create_csv_file_task >> jenkins_trigger
