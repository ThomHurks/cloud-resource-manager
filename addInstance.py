__author__ = 'Thom Hurks and Sander Kools'

import boto3

ec2 = boto3.resource('ec2')
instances = ec2.instances.all()

# when the need for an extra instance arises, start an instance that is turned off
def addInstance():
    # possible state: pending | running | shutting-down | terminated | stopping | stopped
    # if stopped it can be started, print it.
    for instance in instances:
        print(instance.state['Name'])
        if instance.state['Name'] == 'stopped':
            instance.start()
            break

if __name__ == '__main__':
    addInstance()