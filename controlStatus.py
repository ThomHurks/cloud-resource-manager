__author__ = 'Thom Hurks and Sander Kools'

import boto3

ec2 = boto3.resource('ec2')
instances = ec2.instances.all()

accepted_status = {'ok', 'initializing'}

# if one of the instances is impaired, has insufficient-data or is not-applicable, than restart
def controlStatus():
    # status can be ok | impaired | initializing | insufficient-data | not-applicable
    # only ok and initializing is good otherwise reboot
    for instance in instances:
        for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
            if status['InstanceId'] == instance.id:
                print('test')
                if status['InstanceStatus']['Status'] not in accepted_status:
                    print('something is wrong, rebooting')
                    instance.reboot()


if __name__ == '__main__':
    controlStatus()