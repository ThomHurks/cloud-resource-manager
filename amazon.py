__author__ = 'Thom Hurks and Sander Kools'

import boto3
import os
import time
import csv

ec2 = boto3.resource('ec2')
import datetime

print('Default region:')
for instance in ec2.instances.all():
    print(instance.id)

instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])

for instance in instances:
    print(instance.id, instance.instance_type)

for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
    print(status)

print("\nCurrently running instances:")
while True:
    #get status, if status is impaired, insufficient-data or not-applicable, than reboot.
    for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
        print(status['InstanceId'])
        with open('status.csv', 'a') as fapp:
            writer = csv.writer(fapp)
            output = (str('time: ') + str(datetime.datetime.now()))
            output = (output + ' instanceId: ' + status['InstanceId'])
            output = (output + ' status: ' + status['InstanceStatus']['Status'])
            output = (output + ' state: ' + status['InstanceState']['Name'])
            writer.writerow([output])
    time.sleep(60)