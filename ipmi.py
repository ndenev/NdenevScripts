#!/usr/local/bin/python2.7

import re
import subprocess
import time
import socket

HOST = 'graphite.home.lan'
PORT = 2003

def exec_ipmi_sensor():
    return subprocess.check_output(["/usr/local/bin/sudo", "ipmitool", "sensor"])

def get_ipmi_sensor():
    sensor_data = exec_ipmi_sensor()
    data = []
    for line in sensor_data.splitlines():
        line_data = []
        for line_item in line.split('|'):
            line_data.append(line_item.rstrip().lstrip())
        data.append(line_data)
    return data

def parse_metrics(name, value):
    metric = re.sub(r'\s+', '_', name)
    if value.startswith('0x'):
        value = int(value, 16)
    elif value == 'na':
        return (None, None)
    else:
        value = float(value)
    return (metric, value)

def main():
    hostname = socket.gethostname().split('.')[0]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    while True:
        now = int(time.time())
        for item in get_ipmi_sensor():
            (metric, value) = parse_metrics(item[0], item[1])
            if metric and value:
                sock.sendall('{}.ipmi.{} {} {}\n'.format(hostname, metric.lower(), value, now))
                print '{}.ipmi.{} {} {}\n'.format(hostname, metric.lower(), value, now)
        time.sleep(60)

if __name__ == "__main__":
    main()
