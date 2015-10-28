__author__ = 'Thom Hurks and Sander Kools'
import boto3
import os
import time
import csv
import datetime
import addInstance
import stopInstance
import controlStatus


ec2 = boto3.resource('ec2')

print('Default region:')
print('connecting with instances')

instances = ec2.instances.all()


print('Done')


def checkPossibleInstances():
    # possible state: pending | running | shutting-down | terminated | stopping | stopped
    # if stopped it can be started, print it.
    for instance in instances:
        if instance.state['Name'] is 'stopped':
            print('not used instance')
            print(instance.id)


while True:
    print('busy')
    for inst in instances:
        # time
        timeNow = datetime.datetime.now()
        # instance ID
        instanceId = inst.id
        # instance state
        instanceState = inst.state['Name']
        # instance status
        instanceStatus = 'test'
        if instanceState == 'running':
            for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
                if status['InstanceId'] == inst.id:
                    instanceStatus = status['InstanceStatus']['Status']
        else:
            instanceStatus = 'not running'

        with open('status.csv', 'a') as f:
            writer = csv.writer(f)
            output = ('time: ' + str(timeNow) + ' instance ID: ' + str(instanceId) + ' instanceState: ' +
                      str(instanceState) + ' instanceStatus: ' + str(instanceStatus))
            writer.writerow([output])
    # addInstance.addInstance()
    # stopInstance.stopInstance()
    controlStatus.controlStatus()
    time.sleep(60)
