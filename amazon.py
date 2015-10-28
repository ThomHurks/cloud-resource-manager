__author__ = 'Thom Hurks and Sander Kools'
import boto3
import os
import time
import csv
import datetime

ec2 = boto3.resource('ec2')

print('Default region:')
print('connecting with instances')

instances = ec2.instances.all()

accepted_status = {'ok', 'initializing'}

print('Done')


def checkPossibleInstances():
    # possible state: pending | running | shutting-down | terminated | stopping | stopped
    # if stopped it can be started, print it.
    for instance in instances:
        if instance.state['Name'] is 'stopped':
            print('not used instance')
            print(instance.id)


def controlStatus():
    # status can be ok | impaired | initializing | insufficient-data | not-applicable
    # only ok and initializing is good otherwise reboot
    for instance in instances:
        for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
            if status['InstanceId'] == instance.id:
                if status['InstanceStatus']['Status'] not in accepted_status:
                    print('something is wrong, rebooting')
                    instance.reboot()


# when the need for an extra instance arises, start an instance that is turned off
def addInstance():
    # possible state: pending | running | shutting-down | terminated | stopping | stopped
    # if stopped it can be started, print it.
    for instance in instances:
        print(instance.state['Name'])
        if instance.state['Name'] == 'stopped':
            instance.start()
            break

def stopInstance():
    # possible state: pending | running | shutting-down | terminated | stopping | stopped
    # if stopped it can be started, print it.
    for instance in instances:
        print(instance.state['Name'])
        if instance.state['Name'] == 'running':
            instance.stop()
            break

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
    controlStatus()
    addInstance()
    stopInstance()
    time.sleep(60)
