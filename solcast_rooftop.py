#! /usr/bin/env python3


import os
from smllib import SmlStreamReader
from smllib.errors import CrcError

import requests
import time
import argparse
import logging
from datetime import datetime, timedelta
import json

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS



# Parse arguments and environment variables
parser = argparse.ArgumentParser(
    description="Fetch forecast data from solcast and store in InfluxDB."
)
parser.add_argument(
    "--solcasthost",
    type=str,
    default="https://api.solcast.com.au",
    help="Url of the solcast rooftop API",
)
parser.add_argument(
    "--solcast_token",
    type=str,
    default=os.environ.get("SOLCAST_TOKEN", ""),
    help="Solcast token",
)

parser.add_argument(
    "--solcast_site",
    type=str,
    default=os.environ.get("SOLCAST_SITE", ""),
    help="Solcast site",
)

parser.add_argument(
    "--influxdb_forecast_bucket",
    type=str,
    default=os.environ.get("INFLUXDB_FORECAST_BUCKET", "solcast_forecast"),
    help="Name of the InfluxDB database for forecast data.",
)
parser.add_argument(
    "--influxdb_url",
    type=str,
    default=os.environ.get("INFLUXDB_URL", "http://localhost:8086"),
    help="Url of the InfluxDB instance",
)
parser.add_argument(
    "--influxdb_token",
    type=str,
    default=os.environ.get("INFLUX_TOKEN", ""),
    help="InfluxDB token",
)
parser.add_argument(
    "--influxdb_org",
    type=str,
    default=os.environ.get("INFLUX_ORG", "myOrg"),
    help="InfluxDB organization",
)

parser.add_argument(
    "--site_tag",
    type=str,
    default=os.environ.get("SITE_TAG", ""),
    help="Tag of the site used for influxdb",
)

parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Print the data instead of writing to InfluxDB",
)
args, unknown = parser.parse_known_args()

# Set up logging
LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)

def get_forecast(host, site, token):
    headers = {
        'Authorization': f'Bearer {token}'
    }

    response = requests.get(
        f"{host}/rooftop_sites/{site}/forecasts?period=PT30M&format=json", headers=headers
    )
    if response.status_code == 200:
        return response.json()
    else:
        print("Error: {}".format(response.status_code))
        return None


def store_forecast_in_influxdb(
    data, bucket_name: str, url: str, token: str, org: str, site: str
):
    """
    Store the price information in an InfluxDB database.

    :param data: Data to store
    :param bucket: Name of the InfluxDB bucket
    :param url: Url of the InfluxDB instance
    :param token: Token for InfluxDB authentication
    :param org: Organization for InfluxDB
    """

    client = InfluxDBClient(url=url, token=token, org=org)
    try:
        # Create the database if it doesn't exist
        bucket = client.buckets_api().find_bucket_by_name(bucket_name)
        if bucket is None:
            logging.error("Bucket %s does not exist", bucket_name)
            return

        # Convert data to InfluxDB format
        influx_data = []

        for forecast in data:
            influx_data.append(
                {
                    "measurement": "forecast",
                    "tags": {"site": site},
                    "time": forecast["period_start"],
                    "fields": {
                        "estimate": float(forecast["pv_estimate"]),
                        "estimate10": float(forecast["pv_estimate10"]),
                        "estimate90": float(forecast["pv_estimate90"]),
                    },
                }
        )

        # Write data to InfluxDB
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket_name, org=org, record=influx_data)
    except Exception as e:
        logging.error("An error occurred while writing to InfluxDB: %s", e)
    finally:
        client.close()

def parse_forecasts(forecasts_data):
    parsed_forecasts = []
    
    for forecast in forecasts_data['forecasts']:
        # Parse the period_end timestamp
        period_end = datetime.strptime(forecast['period_end'].replace('.0000000Z', '+00:00'), '%Y-%m-%dT%H:%M:%S%z')
        
        # Parse the period (assuming ISO 8601 duration format)
        period_duration = timedelta(minutes=int(forecast['period'][2:-1]))
        
        # Calculate the period_start by subtracting the period duration
        period_start = period_end - period_duration
        
        # Create a new forecast dictionary with the adjusted timestamp
        parsed_forecast = {
            'period_start': period_start.isoformat(),
            'period_end': forecast['period_end'],
            'period': forecast['period'],
            'pv_estimate': forecast['pv_estimate'],
            'pv_estimate10': forecast['pv_estimate10'],
            'pv_estimate90': forecast['pv_estimate90']
        }
        
        parsed_forecasts.append(parsed_forecast)
    
    return parsed_forecasts



# Fetch forecast data
forecast_data = get_forecast(args.solcasthost, args.solcast_site, args.solcast_token)
if forecast_data is None:
    logging.error("Failed to fetch forecast data")
    exit(-1)

#parse forecast data and adjist timestamps
parsed_forecasts = parse_forecasts(forecast_data)

# Write to InfluxDB or print data
if args.dry_run:
    # Print the data instead of writing to InfluxDB
    print(json.dumps(parsed_forecasts, indent=4, sort_keys=True))
    

else:
    store_forecast_in_influxdb(
        parsed_forecasts,
        args.influxdb_forecast_bucket,
        args.influxdb_url,
        args.influxdb_token,
        args.influxdb_org,
        site=args.site_tag,
    )
