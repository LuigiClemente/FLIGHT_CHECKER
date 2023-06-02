import logging
import os
import requests
import json
import time
import csv
import pytz
import scrapy
import subprocess
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from datetime import datetime

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
        "URL", "AIRLINES", "DELAY_THRESHOLD",
        "TIME_TO_DEPARTURE_THRESHOLD", "CANCELLED_FLIGHT_TIME_WINDOW_START",
        "CANCELLED_FLIGHT_TIME_WINDOW_END"
    ]
    for var in required_variables:
        if not os.getenv(var):
            raise ValueError(f"Environment variable {var} is missing or empty")


class FlightScraperManager:
    def __init__(self):
        self.call_immediately = False

    def reset(self):
        """
        Reset the FlightScraperManager and force an immediate Scrapy execution.
        """
        self.call_immediately = True

    def get_next_call_time(self, ongoing_delays, current_time):
        if self.call_immediately or ongoing_delays:
            self.call_immediately = False
            next_call_time = current_time
        else:
            next_call_time = current_time + timedelta(minutes=60)  # Change delay as per your requirement
        return next_call_time


class DelayedScrapyCallSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DelayedScrapyCallSensor, self).__init__(*args, **kwargs)
        self.last_scrapy_call_time = None
        self.flight_scraper_manager = FlightScraperManager()

    def poke(self, context):
        ongoing_delays = context['ti'].xcom_pull(task_ids='analyze_delays_task', key='ongoing_delays')
        current_time = datetime.now()

        # If ongoing_delays is None, make it an empty list
        if ongoing_delays is None:
            ongoing_delays = []

        next_call_time = self.flight_scraper_manager.get_next_call_time(ongoing_delays, current_time)

        # Check if next_call_time is None, if so set it to current_time
        if next_call_time is None:
            next_call_time = current_time

        # Now that next_call_time is not None, the comparison should work
        if self.last_scrapy_call_time is None or self.last_scrapy_call_time < next_call_time:
            if current_time >= next_call_time:
                self.last_scrapy_call_time = current_time  # Record the last successful Scrapy execution time
                return True
            else:
                return False
        else:
            return False

# class FlightSpider(scrapy.Spider):
#     name = "flight_spider"
#     start_urls = ["https://www.amsterdam-airport.com/schiphol-departures"]
#
#     def parse(self, response):
#         # Create a folder to store the file
#         folder_name = "flight_data"
#         if not os.path.exists(folder_name):
#             os.makedirs(folder_name)
#
#         # Save the HTML content to a file
#         html_filename = os.path.join(os.getcwd(), folder_name, "flight_html.html")
#         with open(html_filename, 'wb') as f:
#             f.write(response.body)
#
#         self.log("HTML content saved to file: %s" % html_filename)

class FlightSpider(scrapy.Spider):
    name = "flight_spider"

    def start_requests(self):
        urls = os.getenv("URL").split(",")  # Split URLs by comma
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        flight_data = []

        rows = response.css(".flight-col")

        for row in rows:
            destination = row.css(".flight-col:nth-child(1)::text").get()
            departure = row.css(".flight-col:nth-child(3)::text").get()
            flight_number = row.css(".flight-col:nth-child(4)::text").get()
            airline = row.css(".flight-col:nth-child(5)::text").get()
            status = row.css(".flight-col:nth-child(6)::text").get()

            flight_info = {
                "Destination": destination.strip(),
                "Departure": departure.strip(),
                "Flight": flight_number.strip(),
                "Airline": airline.strip(),
                "Status": status.strip()
            }

            flight_data.append(flight_info)

        # Save flight data to JSON file
        filename = "flight_data.json"
        with open(filename, 'w') as f:
            json.dump(flight_data, f, indent=4)

        self.log("Flight data saved to JSON file: %s" % filename)

        # Filter and display the required flight information
        for flight in flight_data:
            destination = flight["Destination"]
            departure = flight["Departure"]
            flight_number = flight["Flight"]
            airline = flight["Airline"]
            status = flight["Status"]

            print("Destination:", destination)
            print("Departure:", departure)
            print("Flight:", flight_number)
            print("Airline:", airline)
            print("Status:", status)
            print()


class FlightStatusMonitor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.delayInfo = []
        try:
            self.confirm_environment_variables()
            self.retrieve_flight_information()
        except Exception as exception:
            self.logger.error(f"An error occurred during the initialization of FlightStatusMonitor: {str(exception)}")
            raise

    def retrieve_flight_information(self):
        process = subprocess.Popen(['scrapy', 'runspider', 'FlightSpider.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        print("Stdout:", out)
        print("Stderr:", err)

    def fetch_flight_data(self):
        """
        Fetches the flight data by running the scrapy spider.
        Returns a list of flight data.
        """
        logging.basicConfig(level=logging.INFO)

        process = CrawlerProcess(get_project_settings())
        process.crawl(FlightSpider)
        process.start()

        flight_data = []
        try:
            with open("flight_data.json", 'r') as file:
                flight_data = json.load(file)
        except json.JSONDecodeError:
            logging.error("Invalid or empty JSON file: flight_data.json")

        return flight_data

    def write_flight_data_to_json(self, filename):
        """
        Writes the flight data to a JSON file.
        """
        flight_data = self.fetch_flight_data()

        with open(filename, 'w') as file:
            json.dump(flight_data, file, indent=4)

        logging.info(f"Flight data saved to JSON file: {filename}")

    def confirm_environment_variables(self):
        necessary_env_variables = ["URL"]

        for variable in necessary_env_variables:
            if os.getenv(variable) is None:
                raise ValueError(f"The environment variable {variable} is not defined")

    def save_flight_information_to_json(self, filename):
        try:
            with open("flight_info.json") as file:
                flight_info = json.load(file)

            if not flight_info:
                raise ValueError("The flight information is unavailable.")

            with open(filename, 'w') as file:
                json.dump(flight_info, file, indent=4)

        except Exception as exception:
            self.logger.error(f"An error occurred while saving flight information to JSON: {str(exception)}")

    def analyze_delays(self):
        try:
            if self.delayed_data is None:
                self.log.warning("Flight data is not available.")
                return False, []

            ongoing_delays = False
            flights_list = []

            ignored_destinations_bcn = ["https://www.barcelona-airport.com/eng/departures.php"]
            ignored_destinations_ams = ["https://www.amsterdam-airport.com/schiphol-departures"]

            for flight in self.delayed_data:
                if not isinstance(flight, dict):
                    self.log.warning("Invalid flight data found.")
                    continue

                airport = flight.get('dep_iata')
                if not airport:
                    self.log.warning("Airport code not found in flight data.")
                    continue

                if airport == "BCN":
                    ignored_destinations = ignored_destinations_bcn
                elif airport == "AMS":
                    ignored_destinations = ignored_destinations_ams
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
                        dep_time = pytz.utc.localize(dep_time)
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
                            continue

                        self.log.info(f"Flight {flight_iata} is delayed for airport {airport}.")
                        self.notify_plugin("Delayed", flight, airport=airport, flight_iata=flight_iata)

                        if airport in self.last_delay_print_time:
                            self.last_delay_print_time[airport].append(flight_iata)
                        else:
                            self.last_delay_print_time[airport] = [flight_iata]

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


def fetch_flight_data(self):
    """
    Fetches the flight data by running the scrapy spider.
    Returns a list of flight data.
    """
    logging.basicConfig(level=logging.INFO)

    process = CrawlerProcess(get_project_settings())
    process.crawl(FlightSpider)
    process.start()

    with open("flight_data.json", 'r') as file:
        flight_data = json.load(file)

    return flight_data

# Usage example
flight_checker = FlightStatusMonitor()
flight_checker.write_flight_data_to_json("flight_data.json")

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
    delayed_scrapy_call_sensor = DelayedScrapyCallSensor(
        task_id='delayed_scrapy_call_sensor',
        dag=dag,
    )

    load_flight_data_task = PythonOperator(
        task_id='load_flight_data_task',
        python_callable=flight_checker.fetch_flight_data,
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
        job_name=os.getenv("JENKINS_JOB_NAME"),
        jenkins_connection_id=os.getenv("JENKINS_CONNECTION_ID"),
        parameters=json.loads(os.getenv("JENKINS_PARAMETERS", '{"key": "value"}')),
        sleep_time=int(os.getenv("JENKINS_SLEEP_TIME", "30")),
        dag=dag,
    )

    delayed_scrapy_call_sensor >> load_flight_data_task >> analyze_delays_task >> create_csv_file_task >> jenkins_trigger
