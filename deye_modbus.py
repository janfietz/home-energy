#! /usr/bin/env python3

"""
Scrape real-time data from Deye inverter and store in InfluxDB.
"""

import os
import argparse
import logging
import socket
import time
import datetime
import libscrc

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


def build_modbus_read_holding_registers_request_frame(first_reg: int, last_reg: int) -> bytearray:
        reg_count = last_reg - first_reg + 1
        return bytearray.fromhex("0103{:04x}{:04x}".format(first_reg, reg_count))

def parse_modbus_read_holding_registers_response(frame: bytes, first_reg: int, last_reg: int
    ) -> dict[int, bytearray]:
        """
        Parse a response from a modbus read holding registers request. The frame is a bytes object
        containing the bytes of the response, excluding the slave address. The first_reg and last_reg
        arguments are the register addresses requested in the original request. The returned value
        is a dictionary mapping register addresses to the values of the registers.
        """

        # Verify that the frame is long enough to contain the expected number of registers
        reg_count = last_reg - first_reg + 1
        expected_frame_data_len = 2 + 1 + reg_count * 2
        if len(frame) < expected_frame_data_len + 2:  # 2 bytes for crc
            logging.error(f"Modbus frame is too short: {len(frame)} bytes received, {expected_frame_data_len} expected: {frame.hex()}")
            raise ValueError("Modbus frame is too short")

        # Verify that the crc is correct
        actual_crc = int.from_bytes(frame[expected_frame_data_len : expected_frame_data_len + 2], "little")
        expected_crc = libscrc.modbus(frame[0:expected_frame_data_len])
        if actual_crc != expected_crc:
            logging.error(
                "Modbus frame crc is not valid. Expected {:04x}, got {:04x}".format(expected_crc, actual_crc)
            )
            raise ValueError("Modbus frame crc is not valid")
        
        # Parse the register values from the frame
        registers = {}
        a = 0
        while a < reg_count:
            p1 = 3 + (a * 2)
            p2 = p1 + 2
            registers[a + first_reg] = frame[p1:p2]
            a += 1
        return registers

class DeyeAtConnector():
    def __init__(self, host: str, port: int = 48899) -> None:
        self._host = host
        self._port = port
        self.__reachable = True

    def __create_socket(self) -> socket.socket | None:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client_socket.settimeout(1)
            if not self.__reachable:
                self.__reachable = True
                logging.info("Re-connected to socket on IP %s", self._host)
            return client_socket
        except OSError as e:
            if self.__reachable:
                logging.warning("Could not open socket on IP %s: %s", self._host, e)
            else:
                logging.debug("Could not open socket on IP %s: %s", self._host, e)
            self.__reachable = False
            return

    def __send_at_command(self, client_socket: socket, at_command: str) -> None:
        logging.debug("Sending AT command: %s", at_command)
        client_socket.sendto(at_command, (self._host, self._port))
        time.sleep(0.1)

    def __receive_at_response(self, client_socket: socket) -> str:
        attempts = 5
        while attempts > 0:
            attempts = attempts - 1
            try:
                data = client_socket.recv(1024)
                if data:
                    logging.debug("Received AT response in %s. attempt: %s", 5 - attempts, data)
                    return data
                logging.warning("No data received")
            except socket.timeout:
                logging.debug("Connection response timeout")
                if attempts == 0:
                    logging.warning("Too many connection timeouts")
            except OSError as e:
                logging.error("Connection error: %s: %s", self.config.ip_address, e)
                return
            except Exception:
                logging.exception("Unknown connection error")
                return
        return

    def __authenticate(self, client_socket) -> None:
        self.__send_at_command(client_socket, b"WIFIKIT-214028-READ")
        self.__receive_at_response(client_socket)

    def __deauthenticate(self, client_socket) -> None:
        self.__send_at_command(client_socket, b"AT+Q\n")

    def send_request(self, req_frame) -> bytes | None:
        modbus_response = None
        client_socket = self.__create_socket()

        if client_socket is None:
            return None

        try:
            self.__authenticate(client_socket)
            self.__send_at_command(client_socket, b"+ok")

            modbus_frame_str = req_frame.hex()
            self.__send_at_command(
                client_socket, bytes(f"AT+INVDATA={int(len(modbus_frame_str) / 2)},{modbus_frame_str}\n", "ascii")
            )
            time.sleep(1/10.0)
            at_response = self.__receive_at_response(client_socket)
            if not at_response or at_response.startswith(b"+ok=no data"):
                logging.warning(f'No data received for request: {at_response}')
                return None
            if at_response.startswith(b"+ERR="):
                logging.warning(f'Error received for request: {at_response}')
                return None
            if at_response.startswith(b"+ok="):
                modbus_response = DeyeAtConnector.extract_modbus_respose(at_response)
                logging.debug("Extracted Modbus response %s", modbus_response.hex())

            self.__deauthenticate(client_socket)
        except Exception:
            logging.exception("Failed to read data over AT command")
        finally:
            client_socket.close()

        return modbus_response

    @staticmethod
    def extract_modbus_respose(at_cmd_response: bytes) -> bytes:
        extracted_modus_response = at_cmd_response.replace(b"\x10", b"")[4:-4].decode("utf-8")
        if len(extracted_modus_response) > 4 and extracted_modus_response[-4:] == "0000":
            extracted_modus_response = extracted_modus_response[0:-4]
        return bytearray.fromhex(extracted_modus_response)


def read_holding_registers(connector: DeyeAtConnector, first_reg: int, last_reg: int) -> dict[int, bytearray]:
        """Reads multiple modbus holding registers

        Args:
            first_reg (int): The address of the first register to read
            last_reg (int): The address of the last register to read

        Returns:
            dict[int, bytearray]: Map of register values, where the register address is the map key,
            and register value is the map value
        """
        modbus_frame = build_modbus_read_holding_registers_request_frame(first_reg, last_reg)
        modbus_crc = bytearray.fromhex("{:04x}".format(libscrc.modbus(modbus_frame)))
        modbus_crc.reverse()

        modbus_resp_frame = connector.send_request(modbus_frame + modbus_crc)
        if modbus_resp_frame is None:
            return None
        return parse_modbus_read_holding_registers_response(modbus_resp_frame, first_reg, last_reg)

    

'''
fields = [
      { address=60, name="day_energy", scale=0.1, type="UINT16" },
      { address=63, name="total_energy", scale=0.1, type="UINT32" },
      { address=65, name="pv1_day_energy", scale=0.1, type="UINT16" },
      { address=66, name="pv2_day_energy", scale=0.1, type="UINT16" },
      { address=69, name="pv1_total_energy", scale=0.1, type="UINT32" }
      { address=71, name="pv2_total_energy", scale=0.1, type="UINT32" }
      { address=73, name="voltage", scale=0.1, type="INT16" },
      { address=76, name="current", scale=0.1, type="INT16" },
      { address=79, name="freq", scale=0.01, type="INT16" },
      { address=86, name="active_power", scale=0.1, type="INT32" },
      { address=90, name="radiator_temp", scale=0.01, type="INT16" },
      { address=109, name="pv1_voltage", scale=0.1, type="INT16" },
      { address=110, name="pv1_current", scale=0.1, type="INT16" },
      { address=111, name="pv2_voltage", scale=0.1, type="INT16" },
      { address=112, name="pv2_current", scale=0.1, type="INT16" },
    ]
'''

def read_unit16(response: dict[int, bytearray], address: int) -> int:
    return int.from_bytes(response[address], "big") & 0xffff

def read_unit32(response: dict[int, bytearray], address: int) -> int:
    return int.from_bytes(response[address], "big") & 0xffffffff

def read_int16(response: dict[int, bytearray], address: int) -> int:
    value = int.from_bytes(response[address], "big") & 0xffff
    if value > 32767:
        value = value - 65536
    return value

def read_int32(response: dict[int, bytearray], address: int) -> int:
    value = read_unit32(response, address)
    if value > 2147483647:
        value = value - 4294967296
    return value

def get_fields_from_response(response: dict[int, bytearray]) -> dict[str, float]:
    fields = {}
    fields["day_energy"] = read_unit16(response, 60) * 0.1
    fields["uptime"] = read_unit16(response, 60)
    fields["total_energy"] = read_unit32(response, 63) * 0.1
    fields["pv1_day_energy"] = read_unit16(response, 65) * 0.1
    fields["pv2_day_energy"] = read_unit16(response, 66) * 0.1
    fields["pv1_total_energy"] = read_unit32(response, 69) * 0.1
    fields["pv2_total_energy"] = read_unit32(response, 71) * 0.1
    fields["l1_voltage"] = read_int16(response, 73) * 0.1
    fields["l1_current"] = read_int16(response, 76) * 0.1
    fields["freq"] = read_int16(response, 79) * 0.01
    fields["operating_power"] = read_int16(response, 80) * 0.01
    fields["active_power"] = read_int32(response, 86) * 0.1
    fields["radiator_temp"] = read_int16(response, 90) * 0.01
    fields["pv1_voltage"] = read_int16(response, 109) * 0.1
    fields["pv1_current"] = read_int16(response, 110) * 0.1
    fields["pv2_voltage"] = read_int16(response, 111) * 0.1
    fields["pv2_current"] = read_int16(response, 112) * 0.1    
    return fields

def store_in_influxdb(data, bucket_name:str, url:str, token:str, org:str, unit:str):
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
        influx_data.append({
            "measurement": "operation",
            "tags": {
                "pv_unit": unit
            },
            "time": now,
            "fields": {
                "uptime": data['uptime'],
                "operating_power": round(float(data['operating_power']), 2),
                "radiator_temp": round(float(data['radiator_temp']), 2)
            }
        })

        influx_data.append({
            "measurement": "ac",
            "tags": {
                "pv_unit": unit
            },
            "time": now,
            "fields": {
                "day_energy": round(float(data['day_energy']), 2),
                "total_energy": round(float(data['total_energy']), 2),
                "freq": round(float(data['freq']), 2),
                "active_power": round(float(data['active_power']), 2)
            }
        })

        influx_data.append({
            "measurement": "ac",
            "tags": {
                "pv_unit": unit,
                "phase": "l1"
            },
            "time": now,
            "fields": {
                "voltage": round(float(data['l1_voltage']), 2),
                "current": round(float(data['l1_current']), 2)
            }
        })

        # Add PV1 measurement
        influx_data.append({
            "measurement": "dc",
            "tags": {
                "pv_unit": unit,
                "string": "pv1"
            },
            "time": now,
            "fields": {
                "voltage": round(float(data['pv1_voltage']), 2),
                "current": round(float(data['pv1_current']), 2),
                "day_energy": round(float(data['pv1_day_energy']), 2),
                "total_energy": round(float(data['pv1_total_energy']), 2),
            }
        })

        # Add PV2 measurement
        influx_data.append({
            "measurement": "dc",
            "tags": {
                "pv_unit": unit,
                "string": "pv2"
            },
            "time": now,
            "fields": {
                "voltage": round(float(data['pv2_voltage']), 2),
                "current": round(float(data['pv2_current']), 2),
                "day_energy": round(float(data['pv2_day_energy']), 2),
                "total_energy": round(float(data['pv2_total_energy']), 2)
            }
        })
            

        # Write data to InfluxDB
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=bucket_name, org=org, record=influx_data)
    except Exception as e:
        logging.error("An error occurred while writing to InfluxDB: %s", e)
    finally:
        client.close()


# Set up argparse for command line arguments
parser = argparse.ArgumentParser(description='Fetch price information from Tibber API and store in InfluxDB.')
parser.add_argument('--deye_ip', type=str, default=os.environ.get('DEYE_IP'), help='IP address of the Deye inverter')
parser.add_argument('--unit', type=str, help='Name of the Deye inverter unit')
parser.add_argument('--influxdb_bucket', type=str, default=os.environ.get('INFLUXDB_PV_REALTIME_BUCKET', 'pv_realtime'), help='Name of the InfluxDB bucket')
parser.add_argument('--influxdb_url', type=str, default=os.environ.get('INFLUXDB_URL', 'http://localhost:8086'), help='Url of the InfluxDB instance')
parser.add_argument('--influxdb_token', type=str, default=os.environ.get('INFLUX_TOKEN', ''), help='InfluxDB token')
parser.add_argument('--influxdb_org', type=str, default=os.environ.get('INFLUX_ORG', 'myOrg'), help='InfluxDB organization')

args, unknown = parser.parse_known_args()

# Set up logging
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(level=LOGLEVEL)

at_connector = DeyeAtConnector(args.deye_ip)

try:
    response = read_holding_registers(at_connector, 60, 112)
    if response:
        fields = get_fields_from_response(response)
        logging.debug("Fields: %s", fields)

        # do some sanity checks
        if float(fields['pv2_total_energy']) > 0:
            store_in_influxdb(fields, args.influxdb_bucket, args.influxdb_url, args.influxdb_token, args.influxdb_org, args.unit)
        else:
            exit(-1)
    else:
        logging.warning("No response received")
        exit(-1)
except Exception:
    logging.exception("Failed to read data over AT command")



                               