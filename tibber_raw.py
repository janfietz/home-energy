#! /usr/bin/env python3


import os
from smllib import SmlStreamReader
from smllib.errors import CrcError

import requests
from requests.auth import HTTPBasicAuth
import time
import argparse
import logging
import datetime

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

obis_mapping = {
    "0100010800ff": {"name": "energy_total_import", "scale": 0.0001},
    "0100020800ff": {"name": "energy_total_export", "scale": 0.0001},
    "0100100700ff": {"name": "power_total", "scale": 1},
    "0100240700ff": {"name": "power_l1", "scale": 1},
    "0100380700ff": {"name": "power_l2", "scale": 1},
    "01004c0700ff": {"name": "power_l3", "scale": 1},
    "01001f0700ff": {"name": "current_l1", "scale": 0.01},
    "0100330700ff": {"name": "current_l2", "scale": 0.01},
    "0100470700ff": {"name": "current_l3", "scale": 0.01},
    "0100200700ff": {"name": "voltage_l1", "scale": 0.1},
    "0100340700ff": {"name": "voltage_l2", "scale": 0.1},
    "0100480700ff": {"name": "voltage_l3", "scale": 0.1},
    "01000e0700ff": {"name": "frequency", "scale": 0.1},
}

# Parse arguments and environment variables
parser = argparse.ArgumentParser(
    description="Fetch real-time consumption data from Tibber API and store in InfluxDB."
)
parser.add_argument(
    "--tibberhost",
    type=str,
    default=os.environ.get("TIBBERHOST"),
    help="Tibber of pulse host",
)
parser.add_argument(
    "--tibberhost_user",
    type=str,
    default=os.environ.get("TIBBERHOST_USER", "admin"),
    help="Name of tibber host user",
)
parser.add_argument(
    "--tibberhost_password",
    type=str,
    default=os.environ.get("TIBBERHOST_PASSWORD"),
    help="Password of tibber host user",
)
parser.add_argument(
    "--influxdb_realtime_bucket",
    type=str,
    default=os.environ.get("INFLUXDB_REALTIME_BUCKET", "power_realtime"),
    help="Name of the InfluxDB database for realtime data.",
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
args, unknown = parser.parse_known_args()

# Set up logging
LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)

def get_data(host, user, password):
    response = requests.get(
        f"http://{host}/data.json?node_id=1", auth=HTTPBasicAuth(user, password)
    )
    if response.status_code == 200:
        # The response content will be in binary format
        return response.content
    else:
        print("Error: {}".format(response.status_code))
        return None


def store_in_influxdb(
    data, bucket_name: str, url: str, token: str, org: str, meter: str
):
    """
    Store the price information in an InfluxDB database.

    :param data: Data to store
    :param bucket: Name of the InfluxDB bucket
    :param url: Url of the InfluxDB instance
    :param token: Token for InfluxDB authentication
    :param org: Organization for InfluxDB
    """

    now = datetime.datetime.utcnow().isoformat() + "Z"
    client = InfluxDBClient(url=url, token=token, org=org)
    try:
        # Create the database if it doesn't exist
        bucket = client.buckets_api().find_bucket_by_name(bucket_name)
        if bucket is None:
            logging.error("Bucket %s does not exist", bucket_name)
            return

        # Convert data to InfluxDB format
        influx_data = []
        influx_data.append(
            {
                "measurement": "meter_live",
                "tags": {"meter": meter},
                "time": now,
                "fields": data,
            }
        )

        # Write data to InfluxDB
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket_name, org=org, record=influx_data)
    except Exception as e:
        logging.error("An error occurred while writing to InfluxDB: %s", e)
    finally:
        client.close()


while True:
    try:
        data = get_data(args.tibberhost, args.tibberhost_user, args.tibberhost_password)
        if data:
            stream = SmlStreamReader()
            stream.add(data)
            sml_frame = stream.get_frame()
            if sml_frame is None:
                logging.debug(f"Bytes missing: {data}")
            else:
                break
        else:
            time.sleep(0.1)
    except CrcError as e:
        logging.debug(f"CRCError msg: {e.crc_msg} calc: {e.crc_calc}")
        pass
    except Exception as e:
        logging.debug("Error: %s", e)
        pass


# Shortcut to extract all values without parsing the whole frame
obis_values = sml_frame.get_obis()
# apply mapping to obis values
values = {}
meter = ""
for obis in obis_values:
    obis_number = obis.obis
    obis_value = obis.value
    if obis_number in "0100600100ff":
        # parse meter number from string <LF><counter in hex><3 ASCII as HEX><3 digits number><HEX as integer>
        obis_value = obis_value[2:]
        meter_counter = int(obis_value[0:2])
        meter_man = bytes.fromhex(obis_value[2:8]).decode("utf-8")
        meter_unk = obis_value[8:11]
        meter_number = int(obis_value[11:], 16)
        meter = f"{meter_counter}{meter_man}{meter_unk}{meter_number}"

    elif obis_number in obis_mapping:
        values[obis_mapping[obis_number]["name"]] = round(
            obis_value * obis_mapping[obis_number]["scale"], 3
        )

# Write to InfluxDB
store_in_influxdb(
    values,
    args.influxdb_realtime_bucket,
    args.influxdb_url,
    args.influxdb_token,
    args.influxdb_org,
    meter,
)
