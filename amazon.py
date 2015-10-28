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


ec2 = boto3.resource('ec2')
s3 = boto3.resource('s3')
sqs_resource = boto3.resource('sqs')
sqs = sqs_resource.meta.client

print('Default region:')
print('connecting with instances')

instances = ec2.instances.all()


# #create a bucket
def createBucket(bucketName):
    s3.create_bucket(Bucket=bucketName)
    # response = s3.create_bucket(Bucket='bucket31')

def uploadToBucket(bucketName, file):
    s3.Object(bucket_name=bucketName, key=file).put(Body=open(file, 'rb'))
# createBucket('bucket32')

for bucket in s3.buckets.all():
    for key in bucket.objects.all():
        print(key.key)

# obj = s3.Object(bucket_name='bucket20', key='test.py')
# upload to a specific bucket
# uploadToBucket('bucket32', 'test.py')
# obj = s3.Object(bucket_name='bucket31', key='test.py').put(Body=open('test.py', 'rb'))
# obj = s3.Object(bucket_name='bucket31', key='triangle3.txt').put(Body=open('triangle3.txt', 'rb'))

# queue = sqs.create_queue(QueueName='computetest', Attributes={'DelaySeconds': '5'})
# queue = sqs.Queue(url='https://us-west-2.queue.amazonaws.com/853377774032/computetest')
# print(queue.url)
# print(queue.attributes.get('DelaySeconds'))
#
# queue.send_message(MessageBody='hello')

# response = sqs.send_message(QueueUrl='https://us-west-2.queue.amazonaws.com/853377774032/computetest', MessageBody='Hello World')

queue = sqs.get_queue_by_name(QueueName='computetest')
queue.send_message(MessageBody='boto3'’, MessageAttributes={'Author': {'StringValue': 'Daniel','DataType': 'string'
}
})


response = sqs.list_queues()
for url in response.get('QueueUrls', []):
    print(url)

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
