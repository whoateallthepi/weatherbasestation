import re
import time
from datetime import datetime, timezone
import math
import sys
import os
import logging

import collections
import psycopg2

import paho.mqtt.client as mqtt
import random
import json
import base64


class ttnMQTT(object):

    def __init__(self, weather_station, commit=False, **ttn_params):

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())
        self.logger.debug("initialising ttnMQTT object")

        self.client_userdata = {
            'weather_station': weather_station,
            'commit': commit,
            'ttnMQTT': self
        }
        self.client_id = f'python=mqtt-{random.randint(0,1000)}'
        self.ttn_params = ttn_params

    def process_link(self, direction='UPLINK', data='', port = 0):

        # define callbacks

        def on_message(client, userdata, message):
            self = userdata['ttnMQTT']
            self.logger.debug("Message received")
            parsed_json = json.loads(message.payload)
            weather_station = userdata[
                'weather_station']  # weatherStation object is in userdata
            data = weather_station.parse_data(parsed_json)
            self.logger.debug("Decoded data:%s", data)

            if 'No payload' in data:
                self.logger.info("Ignoring message with no payload")
                return

            if userdata['commit']:
                weather_station.commit_data(data)

        def on_connect(client, userdata, flags, rc):
            self = userdata['ttnMQTT']
            if rc == 0:
                self.logger.info("Connected")
            else:
                self.logger.error("Failed to connect")

        def on_subscribe(client, userdata, mid, granted_qos):
            self = userdata['ttnMQTT']
            print("Subscribed\n")

        def on_disconnect(client, userdata, rc):

            self = userdata['ttnMQTT']
            self.logger.info("\nDisconnected with result code %d", rc)
            if rc != 0:
                raise ConnectionError(
                    "Unexpected disconnection from mqtt. Result: " + str(rc))

        def stop(client):
            client.disconnect()

        mqttc = mqtt.Client(self.client_id, userdata=self.client_userdata)

        # assign callbacks

        mqttc.on_connect = on_connect
        mqttc.on_subscribe = on_subscribe
        mqttc.on_message = on_message
        mqttc.on_disconnect = on_disconnect

        #  authenticate

        mqttc.username_pw_set(self.ttn_params['user'],
                              self.ttn_params['password'])

        mqttc.tls_set()

        # connect

        mqttc.connect(self.ttn_params['public_tls_address'],
                      int(self.ttn_params['public_tls_address_port']), 60)

        # subscribe

        # Meaning Quality of Service (QoS)
        # QoS = 0 - at most once
        # The client publishes the message, and there is no acknowledgement by the broker.
        # QoS = 1 - at least once
        # The broker sends an acknowledgement back to the client.
        # The client will re-send until it gets the broker's acknowledgement.
        # QoS = 2 - exactly once
        # Both sender and receiver are sure that the message was sent exactly once, using a kind of handshake

        qos = 0

        if direction == 'UPLINK':

            self.logger.info("Subscribing to topic # with QOS: %d", qos)

            mqttc.subscribe("#", qos)

            try:
                run = True
                while run:
                    mqttc.loop(10)
                    print(".", end="", flush=True)
            except KeyboardInterrupt:
                stop(mqttc)

        elif direction == 'DOWNLINK':  # downlink = TTN >> end_device
            topic = "v3/" + self.ttn_params[
                'user'] + "/devices/" + self.ttn_params[
                    'device_id'] + "/down/push"
            
            self.logger.info("Subscribing to topic %s with QOS: %d", topic,
                             qos)
            
            fport = port
            qos = 0

            #self.logger.info("Sending message via loraWAN port: %d ", port)

            if data != '':
                b64 = base64.b64encode(bytes.fromhex(data)).decode()
                self.logger.debug("Convert hex payload %s to base64 %s", data,b64)
                
            else:
                b64 = 'AA==' # Zero
                

            msg = '{"downlinks":[{"f_port":' + str(fport) + ',"frm_payload":"' + b64 + '","priority": "NORMAL"}]}'
            
            result = mqttc.publish(topic, msg, qos)
            # result: [0, 2]
            status = result[0]
            if status == 0:
                self.logger.info("Send %s to topic %s", msg, topic)
            else:
                self.logger.info("Failed to send message to topic %s",
                                    topic)
                print("Failed to send message to topic " + topic)

class weatherStation(object):

    def _get_stations(self, **postgres_params):
        # gets the details of all the stations from the psql database
        # for future use
        self.logger.debug(
            "getting list of available weather stations from database")
        stations = {}
        try:
            pconn = psycopg2.connect(**postgres_params)
            pcur = pconn.cursor()
            self.logger.debug("PostgreSQL connection open")
            pcur.execute(
                "SELECT id,name, latitude, longitude, altitude, eu_id  FROM weather_station"
            )
            stations = {
                col1: (col2, col3, col4, col5, col6)
                for (col1, col2, col3, col4, col5, col6) in pcur.fetchall()
            }

            # close the communication with the PostgreSQL
            pconn.close()
            self.logger.debug("PostgreSQL connection closed")
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("PostgreSQL error")
            self.logger.error(error)
        finally:
            if pconn is not None:
                pconn.close()
                self.logger.debug('PostgreSQL connection tidied up')

        return stations

    def __init__(self, station_id, **postgres_params):
        # to do - validate ststaion key as 16 characters

        self.VALID_MESSAGES = {
            100: "Weather Report",
            101: "Station report",
            200: "Sync time",
            201: "Update station data"
        }  # The last two are outgoing messages -
        # but we may get them from other base stations

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())
        self.logger.debug("initialising weatherStation object")

        self.stations = self._get_stations(**postgres_params)
        self.station_id = station_id

        self.station_data = self.stations[station_id]

        self.postgres_params = postgres_params

        self.logger.debug("weatherStation initialised")
    
    def _process_2s_complement_wind (self,sensor_direction):
        # works for 3 hex digit integers only (ie up to 4095) 
        # Could develop a more general proc?

        if sensor_direction > 0xcff:  # -ve number 2s complement
                return -1 * (0xfff - sensor_direction + 1)
        else:
            return sensor_direction
                 
    def send_data(self):

        # Generates a '201' message type
        # |0050FA90|FFFD85D1|00BE 
        # ^     ^      ^      ^
        # message lat  long    alt
        # number
        # Both latitude and longitude have five implied decimal places
        # Note message type is now transferred via loraWAN port number


        latitude_int = int(self.station_data[1] * 100000)
        if latitude_int < 0:
            latitude_int = latitude_int + 4294967295  # This is a fudge to generate 2s complement for -ve latitude

        latitude_hex = hex(latitude_int).replace('0x', '').zfill(8)

        longitude_int = int(self.station_data[2] * 100000)
        if longitude_int < 0:
            longitude_int = longitude_int + 4294967295  # This is a fudge to generate 2s complement for -ve longitude

        longitude_hex = hex(longitude_int).replace('0x', '').zfill(8)

        altitude = self.station_data[3]

        if altitude < 0:  # This is a fudge to generate 2s complement for -ve heights - Dead Sea!
            altitude = altitude + 65536

        altitude_hex = hex(altitude).replace('0x', '').zfill(4)

        message = latitude_hex + longitude_hex + altitude_hex

        return message

    # generate a message to sync the time zones

    def sync_time(self):
        # This is a '200' message type
        #  ff
        # ^         ^
        # message  time offset
        # number    (hours only)
        # Note message number now transmitted via loraWAN port number

        # get utc time

        #message_type = hex(200).replace('0x', '')

        time_now = datetime.now().astimezone()

        offset_seconds = time_now.utcoffset().total_seconds()

        # hours offset
        offset = round((offset_seconds) / 3600)

        if offset < 0:  # This is a fudge to generate 2s complement for -ve timezones
            offset = offset + 256

        offset_hex = hex(offset).replace('0x', '').zfill(2)

        return offset_hex

    def reboot(self):
        # This is 203 message type - no data, the 203 is sent via the port number
        # 
        return ''

    def request_data (self):
      # This is 202 mesage type
        # ca
        # ^ Message number

        message_type = hex(202).replace('0x', '')
        return message_type

    def get_station_data(self):

        # This generates a '201' message type
        # C9|0050FA90|FFFD85D1|00BE
        # ^       ^      ^      ^
        # message lat   long    alt
        # number
        # Both latitude and longitude have five implied decimal places
        #
        message_type = hex(201).replace('0x', '')

        # get station_id from the ttn device ID - handy later

        station_id = -99

        dev_eui_upper = mqtt_params['device_id'].partition('eui-')[2].upper()

        for key in self.stations:
            if self.stations[key][4] == dev_eui_upper:
                station_id = key
                break

        latitude_int = int(self.latitude * 100000)
        if latitude_int < 0:
            latitude_int = latitude_int + 4294967295  # This is a fudge to generate 2s complement for -ve latitude

        latitude_hex = hex(latitude_int).replace('0x', '').zfill(8)

        longitude_int = int(self.longitude * 100000)
        if longitude_int < 0:
            longitude_int = longitude_int + 4294967295  # This is a fudge to generate 2s complement for -ve longitude

        longitude_hex = hex(longitude_int).replace('0x', '').zfill(8)

        altitude = self.altitude
        if altitude < 0:  # This is a fudge to generate 2s complement for -ve timezones
            altitude = altitude + 65536

        altitude_hex = hex(altitude).replace('0x', '').zfill(4)

        message = message_type + self.hardware_key + latitude_hex + longitude_hex + altitude_hex

        self._rak811_send_data(message)

    def parse_data(self, parsed_json):
        # define the splits in the data string
        # all message types
        MESSAGE_TYPE = slice(0, 2)

        OFFSET_TIME = slice(0, 8)
        TIMEZONE = slice(8, 10)

        # for weather report - port 100  
        WIND_DIR = slice(10, 13)
        WIND_SPEED = slice(13, 17)
        WIND_GUST = slice(17, 21)
        WIND_GUST_DIR = slice(21, 24)
        WIND_SPEED_2M = slice(24, 28)
        WIND_DIR_2M = slice(28, 31)
        WIND_GUST_10M = slice(31, 35)
        WIND_GUST_10M_DIRECTION = slice(35, 38)
        HUMIDITY = slice(38, 42)
        TEMPERATURE = slice(42, 46)
        RAIN_1H = slice(46, 50)
        RAIN_TODAY = slice(50, 54)
        RAIN_SINCE_LAST = slice(54, 58)
        BAR_UNCORRECTED = slice(58, 62)
        BAR_CORRECTED = slice(62, 66)
        VOLTAGE = slice(66,70) 

        # for station_report - id 101 (0x65)
        LATITUDE = slice(10, 18)
        LONGITUDE = slice(18, 26)
        ALTITUDE = slice(26, 30)


        # Baselines - used to save bytes should be the same as the weather station constants.h file
        BASELINE_PRESSURE = 900.00
        BASELINE_TIME = 1640995200 # 2022-01-01 00:00:00 GMT
        BASELINE_TEMPERATURE = 50.00 

        # get the data from the message
        dev_eui = parsed_json['end_device_ids']['device_id']
        dev_eui_upper = dev_eui.partition('eui-')[2].upper()

        d = collections.OrderedDict()  # for returning the data

        try:
            payload_base64 = parsed_json['uplink_message']['frm_payload']
            payload = base64.b64decode(payload_base64).hex()
        except KeyError:
            self.logger.debug("No payload found")
            d['No payload'] = ''
            return d

        RSSI = parsed_json['uplink_message']['rx_metadata'][0]['rssi']
        SNR = parsed_json['uplink_message']['rx_metadata'][0]['snr']

        d['RSSI'] = RSSI
        d['SNR'] = SNR

        message_type = parsed_json['uplink_message']['f_port']

        if not message_type in self.VALID_MESSAGES:
            d['Unrecognised data'] = payload
            self.logger.warning(
                "Message is not recognised - stopping parse:  %s", payload)
            return d

        d['message_type'] = message_type

        sid = -99  # temp copy of station id for lookup
        for key in self.stations:
            if self.stations[key][4] == dev_eui_upper:
                sid = key
                break
        
        d['station_id'] = sid
        
        d['offset_time'] =  int(payload[OFFSET_TIME], 16) 
        d['timezone'] = int(payload[TIMEZONE], 16)

        ts = datetime.utcfromtimestamp(d['offset_time'] + BASELINE_TIME)
        d['timestamp'] = (ts.strftime('%Y-%m-%d %H:%M:%S')
                          ) + "+00:00"  # time from stations is always UTC

        # process each of the message types

        if message_type == 100:  # weather report

            # A disconnected wind vane returns -1 so allow for this. 
            # Rarely the anenometer will be working and the vane not. So 
            # do this for all direction readings
                        
            d['wind_direction'] = self._process_2s_complement_wind(int(payload[WIND_DIR], 16))

            d['wind_speed'] = int(
                payload[WIND_SPEED],
                16) / 100  # all but directions have 2 implied decimals
            d['wind_gust'] = int(payload[WIND_GUST], 16) / 100
            d['wind_gust_dir'] = self._process_2s_complement_wind(int(payload[WIND_GUST_DIR], 16))
             
            d['wind_speed_avg2m'] = int(payload[WIND_SPEED_2M], 16) / 100
            d['wind_dir_avg2m'] = self._process_2s_complement_wind(int(payload[WIND_DIR_2M], 16))
            d['wind_gust_10m'] = int(payload[WIND_GUST_10M], 16) / 100
            d['wind_gust_dir_10m'] = self._process_2s_complement_wind(int(payload[WIND_GUST_10M_DIRECTION], 16))
            d['humidity'] = int(payload[HUMIDITY], 16) / 100
            temperature = int(payload[TEMPERATURE], 16)

            if temperature > 0xcfff:  # -ve number - 2s complement
                temperature = -1 * (0xffff - temperature + 1)
            d['temperature'] = round (((temperature / 100)  - BASELINE_TEMPERATURE),2) # 2 decimals

            d['rain_1h'] = int(payload[RAIN_1H], 16) / 100
            d['rain_since_last'] = int(payload[RAIN_SINCE_LAST], 16) / 100
            d['rain_today'] = int(payload[RAIN_TODAY], 16) / 100
            d['bar_uncorrected'] = (int(payload[BAR_UNCORRECTED], 16) / 100) + BASELINE_PRESSURE
            d['bar_corrected'] = round(((int(payload[BAR_CORRECTED], 16) / 100) + BASELINE_PRESSURE),2)
            d['voltage'] = (int(payload[VOLTAGE], 16) / 100) 

        elif message_type == 101:  # station report

            latitude = int(payload[LATITUDE], 16)
            if latitude > 0x7fffffff:  #then this i a -ve number
                latitude = -1 * (0xffffffff - latitude + 1)
            d['latitude'] = latitude / 100000  # 5 implied decimals

            longitude = int(payload[LONGITUDE], 16)
            if longitude > 0x7fffffff:  #then this i a -ve number
                longitude = -1 * (0xffffffff - longitude + 1)
            d['longitude'] = longitude / 100000

            altitude = int(payload[ALTITUDE], 16)
            if altitude > 0x7fff:  # ive number
                altitude = -1 * (0xffff - altitude + 1)
            d['altitude'] = altitude  # no decimals

        else:

            self.logger.warning("Message type is not recognised: %i",
                                message_type)
            # raise ValueError('unexpected message type: x' + payload[MESSAGE_TYPE])

        return d

    def commit_data(self, data):

        if 'Unrecognised data' in data:
            self.logger.warning("Attempt to commit 'Unrecognised data':%s",
                                data['Unrecognised data'])
            self.logger.warning("Not committing data to database")
            return

        # only committing weather reports at moment
        if data['message_type'] != 100:
            self.logger.info("Ignoring message type: %i", data['message_type'])
            return

        try:
            pconn = psycopg2.connect(**self.postgres_params)
            cursor = pconn.cursor()
            self.logger.debug("PostgreSQL connection open")

            pquery = ("INSERT INTO weather_reading ( "
                      "reading_time, "
                      "station_id, "
                      "wind_dir, "
                      "wind_speed, "
                      "wind_gust, "
                      "wind_gust_dir, "
                      "wind_speed_avg2m, "
                      "wind_dir_avg2m, "
                      "wind_gust_10m, "
                      "wind_gust_dir_10m, "
                      "humidity, "
                      "temperature, "
                      "rain_1h, "
                      "rain_today, "
                      "rain_since_last, "
                      "bar_uncorrected, "
                      "bar_corrected, "
                      "battery, "
                      "light) "
                      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                      "%s, %s, %s, %s, %s, %s, %s, %s, %s)")

            self.logger.debug('postgreSQL query: %s', pquery)

            pvalues = [
                data['timestamp'],  # reading_time
                data['station_id'],
                data['wind_direction'],  # wind_dir
                data['wind_speed'],  # wind_speed
                data['wind_gust'],  # wind_gust
                data['wind_gust_dir'],  # wind_gust_dir
                data['wind_speed_avg2m'],  # wind_speed_avg2m
                data['wind_dir_avg2m'],  # wind_dir_avg2m
                data['wind_gust_10m'],  # wind_gust_10m
                data['wind_gust_dir_10m'],  # wind_gust_dir_10m
                data['humidity'],  # humidity
                data['temperature'],  # temperature
                data['rain_1h'],  # rain_1h
                data['rain_today'],  # rain_today
                data['rain_since_last'],  # rain_since_last
                data['bar_uncorrected'],  # bar_uncorrected
                data['bar_corrected'],  # bar_corrected
                data['voltage'],  # battery
                0
            ]  # light

            cursor.execute(pquery, pvalues)

            pconn.commit()

            count = cursor.rowcount

            self.logger.debug('%i record inserted into database.', count)

            self.logger.debug("Closing cursor")
            cursor.close()
            self.logger.debug("Cursor closed")

        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("PostgreSQL error")
            self.logger.error(error)
        finally:
            if pconn is not None:
                pconn.close()
                self.logger.debug('PostgreSQL connection tidied up')
