from datetime import datetime
from influxdb_client import Point
from influxdb_client.client.write_api import ASYNCHRONOUS
from dotenv import dotenv_values
from influxdb_client import InfluxDBClient


def parseTxData(message, timestamp):
    tx_data = {
        'imei': message['imei'],
        'serial': message['serial'],
        'momsn': message['momsn'],
        'transmit_time': message['transmit_time'],
        'iridium_latitude': message['iridium_latitude'],
        'iridium_longitude': message['iridium_longitude'],
        'iridium_cep': message['iridium_cep'],
        'timestamp': timestamp
    }

    return tx_data


def parseTextMessage(message):
    data = message['data']
    decoded_data = bytes.fromhex(data).decode('utf-8')
    sep_decoded_data = decoded_data.split(';')

    fields = ['id', 'timestamp', 'lat', 'lon', 'panel_voltage', 'panel_current', 'battery_voltage', 'battery_current',
              'logic_1', 'logic_2', 'logic_3', 'logic_4', 'light_pattern_alarm']

    d = {}

    for field, val in zip(fields, sep_decoded_data):

        if field == 'id':
            d[field] = str(val)
        elif field == 'timestamp':
            d[field] = parseTime(val)
        else:
            d[field] = float(val)

    return d


def parseElectricalMeasurements(d):
    elec_data = {k: d[k] for k in d.keys() if
                 k in ['timestamp', 'panel_voltage', 'panel_current', 'battery_voltage', 'battery_current']}

    return elec_data


def parseEnvironmentalMeasurements(d):
    env_data = {k: d[k] for k in d.keys() if k in ['timestamp', 'lat', 'lon']}

    return env_data


def parseLogicMeasurements(d):
    logic_data = {k: d[k] for k in d.keys() if k in ['timestamp', 'logic_1', 'logic_2', 'logic_3', 'logic_4']}

    return logic_data

def parseAlarmMeasurement(d):
    alarm_data = {k: d[k] for k in d.keys() if k in ['timestamp', 'light_pattern_alarm']}

    return alarm_data


def writeElectricalData(elec_data, api, buoy_id, bucket, org):
    p = Point("Electrical") \
        .tag("Boya", buoy_id) \
        .field("panelVoltage", elec_data['panel_voltage']) \
        .field("panelCurrent", elec_data['panel_current']) \
        .field("batteryVoltage", elec_data['battery_voltage']) \
        .field("batteryCurrent", elec_data['battery_current']) \
        .time(elec_data['timestamp'])

    api.write(bucket=bucket, org=org, record=p)


def writeTxData(tx_data, api, buoy_id, bucket, org):
    p = Point("TX") \
        .tag("Boya", buoy_id) \
        .field("imei", tx_data['imei']) \
        .field("serial", tx_data['serial']) \
        .field("momsn", tx_data['momsn']) \
        .field("transmit_time", tx_data['transmit_time']) \
        .field("iridium_latitude", tx_data['iridium_latitude']) \
        .field("iridium_longitude", tx_data['iridium_longitude']) \
        .field("iridium_cep", tx_data['iridium_cep']) \
        .time(tx_data['timestamp'])

    r = api.write(bucket=bucket, org=org, record=p)
    print(r)


def writeEnvironmentData(env_data, api, buoy_id, bucket, org):
    p = Point("Environment") \
        .tag("Boya", buoy_id) \
        .field("Lat", env_data['lat']) \
        .field("Lon", env_data['lon']) \
        .time(env_data['timestamp'])

    api.write(bucket=bucket, org=org, record=p)


def writeLogicData(logic_data, api, buoy_id, bucket, org):
    p = Point("Logic") \
        .tag("Boya", buoy_id) \
        .field("Logic1", logic_data['logic_1']) \
        .field("Logic2", logic_data['logic_2']) \
        .field("Logic3", logic_data['logic_3']) \
        .field("Logic4", logic_data['logic_4']) \
        .time(logic_data['timestamp'])

    api.write(bucket=bucket, org=org, record=p)


def writeAlarmData(alarm_data, api, buoy_id, bucket, org):
    p = Point("Alarm") \
        .tag("Boya", buoy_id) \
        .field("LightPatternAlarm", alarm_data['light_pattern_alarm']) \
        .time(alarm_data['timestamp'])

    api.write(bucket=bucket, org=org, record=p)


def parseTime(time: str):
    year = int(time[:4])
    month = int(time[4:6])
    day = int(time[6:8])
    hour = int(time[8:10])
    seconds = int(time[10:12])
    minutes = seconds // 60
    seconds = seconds - (minutes*60)


    timestamp = datetime(year=year,
                         month=month,
                         day=day,
                         hour=hour,
                         minute=minutes,
                         second=seconds)

    return timestamp


def postToInflux(data, buoy_id):
    config = dotenv_values(".env")
    client = InfluxDBClient(url=config["URL"],
                            token=config["INFLUXDB_TOKEN"],
                            org=config["ORG"])

    write_api = client.write_api(write_options=ASYNCHRONOUS)

    writeTxData(data['tx_data'], write_api, buoy_id, config["BUCKET"], config["ORG"])
    writeElectricalData(data['elec_data'], write_api, buoy_id, config["BUCKET"], config["ORG"])
    writeLogicData(data['logic_data'], write_api, buoy_id, config["BUCKET"], config["ORG"])
    writeEnvironmentData(data['env_data'], write_api, buoy_id, config["BUCKET"], config["ORG"])
    writeAlarmData(data['alarm_data'], write_api, buoy_id, config["BUCKET"], config["ORG"])

    write_api.__del__()
    client.__del__()


def parseData(message):
    d = parseTextMessage(message)
    elec_data = parseElectricalMeasurements(d)
    env_data = parseEnvironmentalMeasurements(d)
    logic_data = parseLogicMeasurements(d)
    alarm_data = parseAlarmMeasurement(d)
    tx_data = parseTxData(message, d['timestamp'])

    buoy_id = d['id']

    return buoy_id, {
        'tx_data': tx_data,
        'elec_data': elec_data,
        'env_data': env_data,
        'logic_data': logic_data,
        'alarm_data': alarm_data
    }
