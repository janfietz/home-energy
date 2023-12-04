#! /usr/bin/env python3

import os
import argparse
import requests
import base64
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

import logging

def fetch_price_info_today(api_token):
    """
    Fetch today's and tomorrow's price information from the Tibber API.

    :param api_token: API token for authentication
    :return: List of today's and tomorrow's price info records
    """
    try:
        # Endpoint for the Tibber API
        url = 'https://api.tibber.com/v1-beta/gql'
        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }

        # GraphQL query
        query = """
        {
          viewer {
            homes {
              currentSubscription{
                priceInfo{
                  today {
                    total
                    startsAt
                    level
                  }
                  tomorrow {
                    total
                    startsAt
                    level
                  }
                }
              }
            }
          }
        }
        """

        # Make the request
        response = requests.post(url, json={'query': query}, headers=headers)
        data = response.json()

        # Check for errors in response
        if response.status_code != 200 or "errors" in data:
            logging.error("Failed to fetch data: %s", response.text)
            return []

        # Extract today's and tomorrow's price information
        price_info = data['data']['viewer']['homes'][0]['currentSubscription']['priceInfo']
        today_prices = price_info['today']
        tomorrow_prices = price_info['tomorrow']

        return today_prices + tomorrow_prices

    except requests.RequestException as e:
        logging.error("Request failed: %s", e)
        return []
    except Exception as e:
        logging.error("An error occurred: %s", e)
        return []


def fetch_all_price_info(api_token, after_cursor):
    """
    Fetch all price information from the Tibber API.

    :param api_token: API token for authentication
    :param after_cursor: Cursor for pagination in ISO format and base64 encoded
    :return: List of all price info records
    """
    # Endpoint for the Tibber API
    url = 'https://api.tibber.com/v1-beta/gql'
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }

    all_price_info = []

    while True:
        # GraphQL query with pagination
        query = """
        {
          viewer {
            homes {
              currentSubscription {
                priceInfo {
                  range(first: 100, resolution: HOURLY, after: "%s") {
                    pageInfo {
                      endCursor
                      hasNextPage
                      count
                    }
                    nodes {
                      total
                      startsAt
                      level
                    }
                  }
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
            print("Failed to fetch data:", response.text)
            break

        # Extract price information
        price_info = data['data']['viewer']['homes'][0]['currentSubscription']['priceInfo']['range']
        all_price_info.extend(price_info['nodes'])

        # Pagination check
        if not price_info['pageInfo']['hasNextPage']:
            break
        after_cursor = price_info['pageInfo']['endCursor']

    return all_price_info

def store_in_influxdb(data, bucket_name, url, token, org):
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
        for record in data:
            influx_record = {
                "measurement": "energy_prices",
                "tags": {
                    "level": record['level']
                },
                "time": record['startsAt'],
                "fields": {
                    "total": float(record['total'])
                }
            }
            influx_data.append(influx_record)

        # Write data to InfluxDB
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket_name, org=org, record=influx_data)
    except Exception as e:
        logging.error("An error occurred while writing to InfluxDB: %s", e)
    finally:
        client.close()

# Set up argparse for command line arguments
parser = argparse.ArgumentParser(description='Fetch price information from Tibber API and store in InfluxDB.')
parser.add_argument('--api_token', type=str, default=os.environ.get('API_TOKEN'), help='API token for the Tibber API')
parser.add_argument('--timestamp', type=str, default=os.environ.get('TIMESTAMP', "today"), help='Timestamp in ISO format for the after cursor or "today" for today\'s prices')
parser.add_argument('--influxdb_bucket', type=str, default=os.environ.get('INFLUXDB_BUCKET', 'energy'), help='Name of the InfluxDB bucket')
parser.add_argument('--influxdb_url', type=str, default=os.environ.get('INFLUXDB_URL', 'http://localhost:8086'), help='Url of the InfluxDB instance')
parser.add_argument('--influxdb_token', type=str, default=os.environ.get('INFLUX_TOKEN', ''), help='InfluxDB token')
parser.add_argument('--influxdb_org', type=str, default=os.environ.get('INFLUX_ORG', 'myOrg'), help='InfluxDB organization')

args, unknown = parser.parse_known_args()

# Set up logging
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=LOGLEVEL)

# Encode timestamp to base64 for after_cursor
if args.timestamp == "today":
  logging.debug("Fetching today's price information")
  price_info_records = fetch_price_info_today(args.api_token)
else:
  logging.debug("Fetching price information after %s", args.timestamp)
  after_cursor = base64.b64encode(args.timestamp.encode()).decode()
  # Fetch price information
  price_info_records = fetch_all_price_info(args.api_token, after_cursor)

if len(price_info_records) == 0:
    logging.info("No new records to fetch.")
    exit()

logging.info("Fetched %s records", len(price_info_records))

# Store the records in the InfluxDB database
store_in_influxdb(price_info_records, args.influxdb_bucket, args.influxdb_url, args.influxdb_token, args.influxdb_org)

# print end timestamp of last entry
print(f"Fetched {len(price_info_records)} records. Last entry: {price_info_records[-1]['startsAt']}")