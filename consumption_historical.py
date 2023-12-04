#! /usr/bin/env python3
"""
Fetch historical consumption data from Tibber API and store in InfluxDB.
"""
import os
import argparse
import requests
import datetime
import base64
import logging
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

# Set up logging
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=LOGLEVEL)

def query_price_level_for_period(client, db_name, start_time, end_time, org):
    """
    Query the price level data from InfluxDB for a specific period.
    """
    query_api = client.query_api()
    query = f"""from(bucket: "{db_name}")
            |> range(start: {start_time}, stop: {end_time})
            |> filter(fn: (r) => r._measurement == "energy_prices")
            |> filter(fn: (r) => r._field == "total" or r._field == "level")
    """
    
    tables = query_api.query(query, org=org)

    if len(tables) == 0 or len(tables[0].records) == 0:
        logging.warning(f"No price level found for period {start_time} - {end_time}")
        return "NORMAL"
    
    return tables[0].records[0].values['level']


def fetch_historical_consumption(api_token, after_cursor):
    """
    Fetch historical consumption data from the Tibber API until hasNextPage is false.

    :param api_token: API token for authentication
    :param after_cursor: Cursor for pagination in base64 encoded format
    :return: List of all historical consumption data records
    """
    all_consumption_data = []

    while True:
        try:
            # Endpoint for the Tibber API
            url = 'https://api.tibber.com/v1-beta/gql'
            headers = {
                'Authorization': f'Bearer {api_token}',
                'Content-Type': 'application/json'
            }

            # GraphQL query with pagination
            query = """
            {
              viewer {
                homes {
                  consumption(resolution: HOURLY, first: 100, after: "%s") {
                    pageInfo {
                      endCursor
                      hasNextPage
                      count
                    }
                    nodes {
                      from
                      to
                      cost
                      consumption
                      unitPrice
                      unitPriceVAT
                    }
                  }
                }
              }
            }
            """ % after_cursor

            # Make the request
            response = requests.post(url, json={'query': query}, headers=headers)
            data = response.json()

            # Check for errors in response
            if response.status_code != 200 or "errors" in data:
                logging.error("Failed to fetch data: %s", response.text)
                break

            # Extract consumption data
            consumption_data = data['data']['viewer']['homes'][0]['consumption']
            all_consumption_data.extend(consumption_data['nodes'])

            # Pagination check
            if not consumption_data['pageInfo']['hasNextPage']:
                break
            after_cursor = consumption_data['pageInfo']['endCursor']

        except requests.RequestException as e:
            logging.error("Request failed: %s", e)
            break
        except Exception as e:
            logging.error("An error occurred: %s", e)
            break

    return all_consumption_data

def store_in_influxdb(data, bucket_name, url, token, org):
    """
    Store the consumption data in an InfluxDB database.

    :param data: Data to store
    :param db_name: Name of the InfluxDB database
    :param host: Host of the InfluxDB instance
    :param port: Port of the InfluxDB instance
    :param username: Username for InfluxDB
    :param password: Password for InfluxDB
    """
    org = "myOrg"
    try:
        client = InfluxDBClient(url=url, token=token, org=org)

        # Create the database if it doesn't exist
        bucket = client.buckets_api().find_bucket_by_name(bucket_name)
        if bucket is None:
            logging.error("Bucket %s does not exist", bucket_name)
            return


        influx_data = []
        for record in data:

            price_level = query_price_level_for_period(client, bucket_name, record['from'], record['to'], org=org)

            if price_level == 'unknown':
                logging.warning("No price level found for %s", record['from'])
                return
            if float(record['consumption']) is not None:
                influx_record = {
                    "measurement": "historical_consumption",
                    "time": record['from'],
                    "tags": {
                        "level": price_level
                    },
                    "fields": {
                        "cost": float(record['cost']),
                        "consumption": float(record['consumption']),
                        "unitPrice": float(record['unitPrice']),
                        "unitPriceVAT": float(record['unitPriceVAT'])
                    }
                }
                influx_data.append(influx_record)

        # Write data to InfluxDB
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket_name, org=org, record=influx_data)
        logging.info("Historical consumption data written to InfluxDB")

        # Close the client connection
        client.close()

    except Exception as e:
        logging.error("An error occurred while writing to InfluxDB: %s", e)

# Set up argparse for command line arguments
parser = argparse.ArgumentParser(description='Fetch historical consumption data from Tibber API and store in InfluxDB.')
parser.add_argument('--api_token', type=str, default=os.environ.get('API_TOKEN'), help='API token for the Tibber API')
parser.add_argument('--timestamp', type=str, default=(datetime.datetime.now() - datetime.timedelta(days=1)).isoformat(), help='Timestamp in ISO format for the after cursor')
parser.add_argument('--influxdb_bucket', type=str, default=os.environ.get('INFLUXDB_BUCKET', 'energy'), help='Name of the InfluxDB bucket')
parser.add_argument('--influxdb_url', type=str, default=os.environ.get('INFLUXDB_URL', 'http://localhost:8086'), help='Url of the InfluxDB instance')
parser.add_argument('--influxdb_token', type=str, default=os.environ.get('INFLUX_TOKEN', ''), help='InfluxDB token')
parser.add_argument('--influxdb_org', type=str, default=os.environ.get('INFLUX_ORG', 'myOrg'), help='InfluxDB organization')

args, unknown = parser.parse_known_args()

# Encode timestamp to base64 for after_cursor
after_cursor = base64.b64encode(args.timestamp.encode()).decode()

# Fetch historical consumption data
historical_consumption_data = fetch_historical_consumption(args.api_token, after_cursor)
logging.info("Fetched %s historical consumption data records", len(historical_consumption_data))

# Store the records in the InfluxDB database
if historical_consumption_data:
    store_in_influxdb(historical_consumption_data, args.influxdb_bucket, args.influxdb_url, args.influxdb_token, args.influxdb_org)
    logging.info("Historical consumption data stored till %s", historical_consumption_data[-1]['from'])
else:
    logging.info("No historical consumption data available to store in InfluxDB")
