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
        # Here, you should replace the placeholder with your original code
        pass  # Placeholder for your original code

class DelayedApiCallSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DelayedApiCallSensor, self).__init__(*args, **kwargs)
        self.last_api_call_time = None
        self.delay_manager = DelayManager()

    def poke(self, context):
        ongoing_delays = context['ti'].xcom_pull(task_ids='analyze_delays_task', key='ongoing_delays')
        current_time = datetime.now()

        # If ongoing_delays is None, make it an empty list
        if ongoing_delays is None:
            ongoing_delays = []

        next_call_time = self.delay_manager.get_next_call_time(ongoing_delays, current_time)

        # Check if next_call_time is None, if so set it to current_time
        if next_call_time is None:
            next_call_time = current_time

        # Now that next_call_time is not None, the comparison should work
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
            self.airports = os.getenv("AIRPORTS", "BCN,AMS")
            self.airlines = os.getenv("AIRLINES")
            self.delay_threshold = int(os.getenv("DELAY_THRESHOLD", "5"))
            self.time_to_departure_threshold = int(os.getenv("TIME_TO_DEPARTURE_THRESHOLD", "5"))
            self.cancelled_flight_time_window_start = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_START", "5"))
            self.cancelled_flight_time_window_end = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_END", "5"))
            self.api_host = os.getenv("API_HOST", "https://airlabs.co/api/v9")
            self.api_endpoint = os.getenv("API_ENDPOINT", "schedules")
            self.ignored_destinations_bcn = os.getenv("IGNORED_DESTINATIONS_BCN", "").split(",")
            self.ignored_destinations_ams = os.getenv("IGNORED_DESTINATIONS_AMS", "").split(",")
            self.last_delay_print_time = {}  # Stores the last delay print time for each airport

            self.validate_environment_variables()

            self.delayed_data = self.load_flight_data()  # Load flight data upon initializing the FlightChecker class
            if self.delayed_data is None:
                self.log.error("Failed to load flight data.")
                raise ValueError("Failed to load flight data.")

        except Exception as e:
            self.log.error(f"Error initializing FlightChecker: {str(e)}")
            raise

    def validate_environment_variables(self):
        required_env_variables = [
            "FLIGHTS_API_KEY", "AIRPORTS", "AIRLINES", "DELAY_THRESHOLD",
            "TIME_TO_DEPARTURE_THRESHOLD", "CANCELLED_FLIGHT_TIME_WINDOW_START",
            "CANCELLED_FLIGHT_TIME_WINDOW_END", "API_HOST", "API_ENDPOINT",
            "IGNORED_DESTINATIONS_BCN", "IGNORED_DESTINATIONS_AMS"
        ]

        for var in required_env_variables:
            if os.getenv(var) is None:
                raise ValueError(f"Environment variable {var} is not set")

    def load_flight_data(self):
        try:
            airports = self.airports.split(",")  # Split airports by comma
            bcns = [airport for airport in airports if airport == "BCN"]  # Get BCN from airports
            url = f"{self.api_host}/{self.api_endpoint}?dep_iata={','.join(bcns)}&api_key={self.api_key}"

            response = requests.get(url)
            response.raise_for_status()
            api_data = response.json()  # Get the entire JSON response from the API

            # Store the API response as JSON in the file
            filepath = "flights.json"
            with open(filepath, 'w') as file:
                json.dump(api_data, file)

            # Check the size of the file and delete if it exceeds 1 GB
            max_file_size = 1 * 1024 * 1024 * 1024  # 1 GB in bytes
            if os.path.getsize(filepath) > max_file_size:
                os.remove(filepath)
                logging.warning(f"File {filepath} exceeded the size limit and was deleted.")

            return api_data

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to load flight data from API: {str(e)}")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding API response JSON: {str(e)}")
        except Exception as e:
            logging.error(f"Error loading flight data: {str(e)}")

        return None  # Return None when an error occurs

    def analyze_delays(self):
        try:
            if self.delayed_data is None:
                self.log.warning("Flight data is not available.")
                return False, []

            ongoing_delays = False
            flights_list = []

            for flight in self.delayed_data:
                if not isinstance(flight, dict):
                    self.log.warning("Invalid flight data found.")
                    continue

                airport = flight.get('dep_iata')
                if not airport:
                    self.log.warning("Airport code not found in flight data.")
                    continue

                if airport == "BCN":
                    ignored_destinations = self.ignored_destinations_bcn
                elif airport == "AMS":
                    ignored_destinations = self.ignored_destinations_ams
                else:
                    continue

                airline_iata = flight.get('airline_iata')
                arr_iata = flight.get('arr_iata')
                dep_time_str = flight.get('dep_time')
                dep_delayed = flight.get('dep_delayed')
                status = flight.get('status')

                if (
                    airline_iata and arr_iata and dep_time_str and dep_delayed and status and
                    airline_iata in self.airlines.split(",") and arr_iata not in ignored_destinations
                ):
                    try:
                        dep_time = datetime.strptime(dep_time_str, "%Y-%m-%d %H:%M")
                        dep_time = pytz.utc.localize(dep_time)  # Convert departure time to UTC
                    except ValueError:
                        self.log.warning(f"Invalid departure time format for flight: {flight}")
                        continue

                    if (
                        dep_delayed > self.delay_threshold and
                        dep_time > datetime.now(pytz.utc) + timedelta(minutes=self.time_to_departure_threshold)
                    ):
                        ongoing_delays = True

                        flight_iata = flight.get('flight_iata')
                        if not flight_iata:
                            self.log.warning("Flight IATA code not found in flight data.")
                            continue

                        if airport in self.last_delay_print_time and flight_iata in self.last_delay_print_time[airport]:
                            continue  # Skip already processed delays

                        self.log.info(f"Flight {flight_iata} is delayed for airport {airport}.")
                        self.notify_plugin("Delayed", flight, airport=airport, flight_iata=flight_iata)

                        # Update last delay print time
                        if airport in self.last_delay_print_time:
                            self.last_delay_print_time[airport].append(flight_iata)
                        else:
                            self.last_delay_print_time[airport] = [flight_iata]

                        # Only acknowledge a cancelled flight if a delay has been printed for the same airport
                        if status == "cancelled":
                            time_since_last_delay = (
                                datetime.now(pytz.utc) - self.last_delay_print_time[airport][-1]
                            ).total_seconds() / 60
                            if (
                                self.cancelled_flight_time_window_start < time_since_last_delay <
                                self.cancelled_flight_time_window_end
                            ):
                                self.log.info(f"Flight {flight_iata} is cancelled for airport {airport}.")
                                self.notify_plugin("Cancelled", flight, airport=airport, flight_iata=flight_iata)

                        # Add flight to the list for CSV creation
                        flights_list.append({
                            'Flight Number': flight_iata,
                            'Departure Airport': airport,
                            'Departure Time': dep_time_str,
                            'Delay (Minutes)': dep_delayed,
                            'Status': status,
                            'Airline': airline_iata,
                            'Destination': arr_iata
                        })

            return ongoing_delays, flights_list

        except Exception as e:
            self.log.error(f"Error analyzing delays: {str(e)}")
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
            
flight_checker = FlightChecker()
flight_checker.analyze_delays()

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
