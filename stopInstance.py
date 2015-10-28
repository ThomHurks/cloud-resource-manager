__author__ = 'Thom Hurks and Sander Kools'

import boto3

ec2 = boto3.resource('ec2')
instances = ec2.instances.all()

# when there is enough load for an instance less, stop one of the instances.
def stopInstance():
    # possible state: pending | running | shutting-down | terminated | stopping | stopped
    # if stopped it can be started, print it.
    for instance in instances:
        print(instance.state['Name'])
        if instance.state['Name'] == 'running':
            instance.stop()
            break


if __name__ == '__main__':
    stopInstance()