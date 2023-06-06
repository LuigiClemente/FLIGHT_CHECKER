import os
import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse
from pathlib import Path
from requests_html import HTMLSession
from pytz import timezone

from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import timedelta

load_dotenv()

def validate_environment_variables():
    required_variables = [
        "URL", "AIRLINES", "TIME_TO_DEPARTURE_THRESHOLD", "CANCELLED_FLIGHT_TIME_WINDOW_START",
        "CANCELLED_FLIGHT_TIME_WINDOW_END"
    ]
    for var in required_variables:
        if not os.getenv(var):
            raise ValueError(f"Environment variable {var} is missing or empty")

class FlightStatusMonitor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.cet = timezone('CET')

    def retrieve_flight_schedules(self):
        session = HTMLSession()
        url = os.getenv("URL")
        if url is None:
            raise ValueError("Environment variable 'URL' is not defined")

        response = session.get(url)
        rows = response.html.find(".flight-row")
        schedules_data = []
        for row in rows:
            destination_element = row.find(".flight-col__dest b", first=True)
            departure_element = row.find(".flight-col__hour", first=True)
            flight_numbers = [a.text.strip() for a in row.find(".flight-col__flight a")]
            airlines = [a.text.strip() for a in row.find(".flight-col__airline a")]

            destination = destination_element.text.strip() if destination_element else None
            departure = departure_element.text.strip() if departure_element else None

            if destination and departure and flight_numbers and airlines:
                first_flight_number = flight_numbers[0] if flight_numbers else None
                first_airline_name = airlines[0].split()[0] if airlines else None

                scraping_time = (datetime.now(self.cet) + timedelta(minutes=int(os.getenv("TIME_TO_DEPARTURE_THRESHOLD")))).strftime('%H:%M')
                scraping_url = url + "/" + first_flight_number

                try:
                    departure_time = datetime.strptime(departure, '%H:%M').replace(tzinfo=self.cet)
                except ValueError:
                    logging.warning("Skipping row due to invalid departure time format: %s" % row.html)
                    continue

                schedule_scraping_time = (departure_time + timedelta(minutes=int(os.getenv("DELAY_THRESHOLD")))).strftime('%H:%M')

                schedule_info = {
                    "Destination": destination,
                    "Departure": departure,
                    "FlightNumber": first_flight_number,
                    "Airline": first_airline_name,
                    "ScrapingTime": scraping_time,
                    "ScrapingURL": scraping_url,
                    "Status": "Scheduled [+]",
                    "ScheduleScraping": schedule_scraping_time
                }
                schedules_data.append(schedule_info)

        flight_data_dir = Path('flight_data')
        flight_data_dir.mkdir(parents=True, exist_ok=True)

        date = datetime.now(self.cet).strftime('%d_%m_%Y')
        filename = f"flight_data/{date}.schedules.amsterdam-airport.com_schiphol-departures.json"
        with open(filename, 'w') as f:
            json.dump(schedules_data, f, indent=4)

        logging.info("Flight schedules saved to JSON file: %s" % filename)

flight_checker = FlightStatusMonitor()

default_args = {
    'start_date': datetime.strptime(os.getenv("DAG_START_DATE", "2023-05-21"), "%Y-%m-%d"),
    'retries': int(os.getenv("DAG_RETRIES", "1")),
    'retry_delay': timedelta(minutes=int(os.getenv("DAG_RETRY_DELAY", "1"))),
    'catchup': False,
}

with DAG(
    'flight_checker',
    default_args=default_args,
    schedule_interval='1 0,6,12,18 * * *',
    max_active_runs=1,
) as dag:
    validate_environment_variables()

    retrieve_flight_schedules_task = PythonOperator(
        task_id='retrieve_flight_schedules_task',
        python_callable=flight_checker.retrieve_flight_schedules,
        dag=dag,
    )

    retrieve_flight_schedules_task

