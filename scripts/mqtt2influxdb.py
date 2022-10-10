#!/usr/bin/env python

from influxdb import InfluxDBClient
from paho.mqtt.client import Client as MQTTClient
import re
import sys
import typing

INFLUXDB_HOST     =
INFLUXDB_PORT     =
INFLUXDB_USERNAME =
INFLUXDB_PASSWORD =
INFLUXDB_DATABASE =
MQTT_CLIENT_ID    =
MQTT_USERNAME     =
MQTT_PASSWORD     =
MQTT_HOST         =
MQTT_PORT         =
MQTT_TOPIC_ROOT   = 'ab123456'
MQTT_TOPIC        = MQTT_TOPIC_ROOT + '/+/+'
MQTT_REGEX        = MQTT_TOPIC_ROOT + '/([^/]+)/([^/]+)'

class SensorData(typing.NamedTuple):
    sensor_name: str
    measurement: str
    value: float

def parse_message(topic, payload):
    match = re.match(MQTT_REGEX, topic)
    if match:
        sensor_name = match.group(1)
        measurement = match.group(2)
        return SensorData(sensor_name, measurement, float(payload))
    else:
        return None

def store_data(data, db_client):
    json_data = [
            {
                'measurement': data.measurement,
                'tags': {
                    'sensor_name': data.sensor_name
                    },
                'fields': {
                    'value': data.value
                    }
                }
            ]
    db_client.write_points(json_data)

def mqtt_connect_callback(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)

def mqtt_message_callback(client, userdata, msg):
    print(msg.topic + ' ' + str(msg.payload))
    sensor_data = parse_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data:
        store_data(sensor_data, userdata)

def main():
    influxdb_client = InfluxDBClient(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUXDB_DATABASE)
    mqtt_client = MQTTClient( MQTT_CLIENT_ID, userdata=influxdb_client)
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.tls_set()
    mqtt_client.on_connect = mqtt_connect_callback
    mqtt_client.on_message = mqtt_message_callback
    mqtt_client.connect(MQTT_HOST, MQTT_PORT)
    mqtt_client.loop_forever()

if __name__ == '__main__':
    sys.exit(main())
