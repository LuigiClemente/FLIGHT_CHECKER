# Add the required imports for the changes
import os
import requests
import logging
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Validate Input Data
def validate_flight_data(flight_data):
    """
    Validates the structure and contents of flight_data.
    Raises an error if any sub-list has less than 8 elements.
    """
    for flight in flight_data:
        if len(flight) < 8:
            raise ValueError(f"Invalid flight data: {flight}")


# Handle Missing Environment Variables
def get_env_variable(name):
    """
    Retrieves the value of the specified environment variable.
    Raises an error if the variable is missing or empty.
    """
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Environment variable {name} is missing")
    return value


# Convert API Response to JSON
def get_json_response(url):
    """
    Sends a GET request to the specified URL and retrieves the JSON response.
    Raises an error if the response is not a valid JSON or if the request fails.
    """
    response = requests.get(url)
    response.raise_for_status()  # This will raise an exception for 4xx and 5xx status codes
    try:
        return response.json()
    except ValueError:
        raise ValueError(f"Unable to parse API response to JSON: {response.text}")


# Set required environment variables
FLIGHTS_API_KEY = get_env_variable("FLIGHTS_API_KEY")
AIRPORTS = get_env_variable("AIRPORTS").split(",")
AIRLINES = get_env_variable("AIRLINES").split(",")
DELAY_THRESHOLD = int(get_env_variable("DELAY_THRESHOLD"))
TIME_TO_DEPARTURE_THRESHOLD = int(get_env_variable("TIME_TO_DEPARTURE_THRESHOLD"))
CANCELLED_FLIGHT_TIME_WINDOW_START = int(get_env_variable("CANCELLED_FLIGHT_TIME_WINDOW_START"))
CANCELLED_FLIGHT_TIME_WINDOW_END = int(get_env_variable("CANCELLED_FLIGHT_TIME_WINDOW_END"))
API_HOST = get_env_variable("API_HOST")
API_ENDPOINT = get_env_variable("API_ENDPOINT")
IGNORED_DESTINATIONS_BCN = get_env_variable("IGNORED_DESTINATIONS_BCN").split(",")
IGNORED_DESTINATIONS_AMS = get_env_variable("IGNORED_DESTINATIONS_AMS").split(",")
JENKINS_JOB_NAME = get_env_variable("JENKINS_JOB_NAME")
JENKINS_CONNECTION_ID = get_env_variable("JENKINS_CONNECTION_ID")
CSV_FILE_NAME = get_env_variable("CSV_FILE_NAME")
API_CALL_TIME_ONGOING_DELAYS = int(get_env_variable("API_CALL_TIME_ONGOING_DELAYS"))
API_CALL_TIME_BEFORE_7AM = int(get_env_variable("API_CALL_TIME_BEFORE_7AM"))
API_CALL_TIME_DEFAULT = int(get_env_variable("API_CALL_TIME_DEFAULT"))
DAG_START_DATE = datetime.strptime(get_env_variable("DAG_START_DATE"), "%Y-%m-%d")
DAG_RETRIES = int(get_env_variable("DAG_RETRIES"))
DAG_RETRY_DELAY = timedelta(minutes=int(get_env_variable("DAG_RETRY_DELAY")))
JENKINS_PARAMETERS = json.loads(get_env_variable("JENKINS_PARAMETERS"))
JENKINS_SLEEP_TIME = int(get_env_variable("JENKINS_SLEEP_TIME"))
JENKINS_MAX_TRIES = int(get_env_variable("JENKINS_MAX_TRIES"))
JENKINS_ALLOWED_STATES = get_env_variable("JENKINS_ALLOWED_STATES").split(",")

# Define the FlightChecker class
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
            url = f"{self.api_host}/{self.api_endpoint}?dep_iata={','.join(self.airports)}&api_key={self.api_key}"
            retry_count = 0
            max_retries = API_MAX_RETRIES
            while retry_count < max_retries:
                try:
                    flight_data = get_json_response(url)
                    validate_flight_data(flight_data)
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
                if len(flight) < 8:
                    logging.error(f"Invalid flight data: {flight}")
                    continue

                airport = flight[0]
                if airport == "BCN":
                    ignored_destinations = self.ignored_destinations_bcn
                elif airport == "AMS":
                    ignored_destinations = self.ignored_destinations_ams
                else:
                    continue

                if flight[1] in self.airlines and flight[7] not in ignored_destinations:
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
        self.flight_checker = FlightChecker()

    def poke(self, context):
        ongoing_delays = context['ti'].xcom_pull(task_ids='analyze_delays_task', key='ongoing_delays')
        current_time = datetime.now()
        next_call_time = self.flight_checker.get_next_call_time(ongoing_delays, current_time)

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

    trigger_jenkins_job = JenkinsJobTriggerOperator(
        task_id='trigger_jenkins_job',
        job_name=JENKINS_JOB_NAME,
        parameters=JENKINS_PARAMETERS,
        jenkins_connection_id=JENKINS_CONNECTION_ID,
        sleep_time=JENKINS_SLEEP_TIME,
        max_try_before_job_appears=JENKINS_MAX_TRIES,
        allowed_jenkins_states=JENKINS_ALLOWED_STATES,
        dag=dag,
    )

delayed_api_call_sensor >> load_flight_data_task >> analyze_delays_task >> create_csv_file_task >> trigger_jenkins_job
