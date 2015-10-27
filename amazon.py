__author__ = 'Thom Hurks'

import boto3
import os

ec2 = boto3.resource('ec2')

def getStatus():
    i = 0
    opts = {}
    for inst in instances:
        opts[i] = (inst.id, inst.instance_type, inst.public_dns_name, inst.launch_time)
        print("\t%d:" % i + " %s - %s : %s (Running since: %s)" % opts[i])
        i += 1


print('Default region:')
for instance in ec2.instances.all():
    print(instance.id)

instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])


for instance in instances:
    print(instance.id, instance.instance_type)


for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
    print(status)


print("\nCurrently running instances:")
getStatus()