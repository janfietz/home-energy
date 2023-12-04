#! /usr/bin/env python3

"""
Script to fetch real-time consumption data from Tibber API and store in InfluxDB.
"""

import os
import argparse
import asyncio
import aiohttp
import tibber
import datetime
import logging
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Parse arguments and environment variables
parser = argparse.ArgumentParser(description='Fetch real-time consumption data from Tibber API and store in InfluxDB.')
parser.add_argument('--api_token', type=str, default=os.environ.get('API_TOKEN'), help='API token for the Tibber API')
parser.add_argument('--influxdb_realtime_bucket', type=str, default=os.environ.get('INFLUXDB_REALTIME_BUCKET', 'power_realtime'), help='Name of the InfluxDB database for realtime data.')
parser.add_argument('--influxdb_url', type=str, default=os.environ.get('INFLUXDB_URL', 'http://localhost:8086'), help='Url of the InfluxDB instance')
parser.add_argument('--influxdb_token', type=str, default=os.environ.get('INFLUX_TOKEN', ''), help='InfluxDB token')
parser.add_argument('--influxdb_org', type=str, default=os.environ.get('INFLUX_ORG', 'myOrg'), help='InfluxDB organization')
args, unknown = parser.parse_known_args()

# Set up logging
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=LOGLEVEL)

# Global variable for last time data was received for emergency shutdown
last_time_data_was_received = datetime.datetime.now()

# Tibber connection
async def connect_tibber():
    async with aiohttp.ClientSession() as session:
        tibber_connection = tibber.Tibber(args.api_token, websession=session, user_agent="TibberCrawler/1.0")
        await tibber_connection.update_info()
        home = tibber_connection.get_homes()[0]
        # if not home.features.realTimeConsumptionEnabled:
        #     logging.error("Real-time consumption not enabled for this home.")
        #     #return
        await home.rt_subscribe(data_callback)

# InfluxDB client setup
client = InfluxDBClient(url=args.influxdb_url, token=args.influxdb_token, org=args.influxdb_org)

 # Create the database if it doesn't exist
bucket = client.buckets_api().find_bucket_by_name(args.influxdb_realtime_bucket)
if bucket is None:
    logging.error("Bucket %s does not exist", args.influxdb_realtime_bucket)
    exit(-1)

# Callback for real-time data
def data_callback(pkg):
    logging.debug("Data received from Tibber")
    data = pkg.get("data")
    if data is None:
        logging.Warning("No data object in package.")
        return
    measurement = data.get("liveMeasurement")
    if measurement is not None:
        json_body = [
            {
                "measurement": "tibber_LiveMeasurement",
                "time": measurement['timestamp'],
                "fields": {
                    "power" : float(measurement['power']),
                    "powerProduction" : float(measurement['powerProduction']),
                    "lastMeterConsumption" : float(measurement['lastMeterConsumption']),
                    "lastMeterProduction" : float(measurement['lastMeterProduction']),
                    "accumulatedConsumption" : float(measurement['accumulatedConsumption']),
                    "accumulatedProduction" : float(measurement['accumulatedProduction']),
                    "accumulatedConsumptionLastHour" : float(measurement['accumulatedConsumptionLastHour']),
                    "accumulatedProductionLastHour" : float(measurement['accumulatedProductionLastHour'])
                }

            }
        ]
        try:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            write_api.write(bucket=args.influxdb_realtime_bucket, org=args.influxdb_org, record=json_body)

            logging.debug("Data written to InfluxDB")
        except Exception as e:
            logging.warning("Error while writing to InfluxDB: {}".format(e))
            exit(-1)
    else:
        logging.Warning("No liveMeasurement in data")

    # Update global variable for last time data was received
    global last_time_data_was_received
    last_time_data_was_received = datetime.datetime.now()    


# Main loop with reconnect logic
async def main():
    await connect_tibber()
    while True:
        try:            
            await asyncio.sleep(10)

            # Check if data has been received within the last 60 seconds
            if (datetime.datetime.now() - last_time_data_was_received).total_seconds() > 60:
                logging.error("No data received within the last 60 seconds. Exiting.")
                exit(-1)
        except Exception as e:
            logging.error(f"Error: {e}")
            exit(-1)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
