__author__ = 'Thom Hurks and Sander Kools'
import boto3
import os
import time
import csv
import datetime
import botocore
import addInstance
import stopInstance
import controlStatus
from subprocess import call


ec2 = boto3.resource('ec2')
s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')

print('Default region:')
print('connecting with instances')

instances = ec2.instances.all()

for instance in instances:
    print(instance.public_dns_name)

# #create a bucket
def createBucket(bucketName):
    s3.create_bucket(Bucket=bucketName)
    # response = s3.create_bucket(Bucket='bucket31')

def uploadToBucket(bucketName, file):
    s3.Object(bucket_name=bucketName, key=file).put(Body=open(file, 'rb'))
# createBucket('bucket32')

call(["ssh", "-i", "D:\Documents\Github\gridAndCloud\key\Grabot.pem", "ec2-user@ec2-52-26-179-116.us-west-2.compute.amazonaws.com", "python test.py"])


def checkPossibleInstances():
    # possible state: pending | running | shutting-down | terminated | stopping | stopped
    # if stopped it can be started, print it.
    for instance in instances:
        if instance.state['Name'] is 'stopped':
            print('not used instance')
            print(instance.id)


# while True:
#     print('busy')
#
#     for inst in instances:
#         # time
#         timeNow = datetime.datetime.now()
#         # instance ID
#         instanceId = inst.id
#         # instance state
#         instanceState = inst.state['Name']
#         # instance status
#         instanceStatus = 'test'
#         if instanceState == 'running':
#             for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
#                 if status['InstanceId'] == inst.id:
#                     instanceStatus = status['InstanceStatus']['Status']
#         else:
#             instanceStatus = 'not running'
#
#         with open('status.csv', 'a') as f:
#             writer = csv.writer(f)
#             output = ('time: ' + str(timeNow) + ' instance ID: ' + str(instanceId) + ' instanceState: ' +
#                       str(instanceState) + ' instanceStatus: ' + str(instanceStatus))
#             writer.writerow([output])
#     # addInstance.addInstance()
#     # stopInstance.stopInstance()
#     # controlStatus.controlStatus()
#     time.sleep(60)
