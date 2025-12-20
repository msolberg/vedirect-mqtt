# Copyright Michael Solberg <mpsolberg@gmail.com>
# Based on AWS IOT SDK samples:
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

import configparser
from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import sys
import time
import json
import serial
from serial.tools import list_ports

config = configparser.ConfigParser()
config.read('vedirect-mqtt.ini')

endpoint=config['broker']['endpoint']
cert_filepath=config['broker']['cert_filepath']
pri_key_filepath=config['broker']['pri_key_filepath']
ca_filepath=config['broker']['ca_filepath']
clientId=config['broker']['clientId']
message_topic=config['broker']['message_topic']

ve_labels = {
    'PID': 'process_id',
    'FW': 'firmware',
    'SER#': 'serial_num',
    'V': 'battery_voltage',
    'I': 'battery_current',
    'VPV': 'panel_voltage',
    'PPV': 'panel_power',
    'CS': 'state_of_operation',
    'MPPT': 'tracker_operation_mode',
    'OR': 'off_reason',
    'ERR': 'error_code',
    'LOAD': 'load_output_state',
    'IL': 'load_current',
    'H19': 'yield_total',
    'H20': 'yield_today',
    'H21': 'maximum_power_today',
    'H22': 'yield_yesterday',
    'H23': 'maximum_power_yesterday',
    'HSDS': 'day_sequence_number'
    }

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))

# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)
    

def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailureData)
    print("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    print("Connection closed")

def find_vedirect_port():
    """
    Finds the serial port associated with a Victron VE.Direct device.
    """
    vedirect_port = None
    com_ports_list = list(list_ports.comports())
    for port in com_ports_list:
        # Look for common identifiers for VE.Direct USB adapters
        if "VE.Direct" in port.description or "ttyUSB" in port.device:
            vedirect_port = port.device
            break
    return vedirect_port

def read_victron_data(port_name, baud_rate=19200):
    """
    Reads data from the specified serial port, assuming VE.Direct protocol.
    """
    data = {}
    
    try:
        ser = serial.Serial(port_name, baud_rate, timeout=1)
        print(f"Connected to {port_name} at {baud_rate} baud.")
        chunk = 0
        while chunk < 2:
            line = ser.readline().decode('utf-8', errors='ignore').strip()
            if line:
                output = line.split()
                if output[0] == 'Checksum':
                    chunk = chunk + 1
                if output[0] in ve_labels.keys():
                    if chunk:
                        print(ve_labels[output[0]],':',output[1])
                        data[ve_labels[output[0]]] = output[1]
            time.sleep(0.1) # Small delay to prevent busy-waiting
            
    except serial.SerialException as e:
        print(f"Error opening or reading serial port: {e}")
    finally:
        if 'ser' in locals() and ser.is_open:
            ser.close()
            print("Serial port closed.")
            return data

if __name__ == '__main__':
    # Figure out which port we're listening on
    port = find_vedirect_port()
    if port:
        print(f"VE.Direct device found on: {port}")
        read_victron_data(port)
    else:
        print("No VE.Direct device found. Please ensure it's connected.")
        sys.exit(1)
    
    # Create a MQTT connection from the command line data
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=endpoint,
        cert_filepath=cert_filepath,
        pri_key_filepath=pri_key_filepath,
        ca_filepath=ca_filepath,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=clientId,
        clean_session=False,
        keep_alive_secs=30,
        on_connection_success=on_connection_success,
        on_connection_failure=on_connection_failure,
        on_connection_closed=on_connection_closed)

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    while True:
        data = read_victron_data(port)
        message = json.dumps(data)
        if 'firmware' in data.keys():
            print("Publishing message to topic '{}': {}".format(message_topic, message))
            mqtt_connection.publish(
                topic=message_topic,
                payload=message,
                qos=mqtt.QoS.AT_LEAST_ONCE)
        else:
            print("Failed to retrieve data from sensors")
        time.sleep(5)

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")
