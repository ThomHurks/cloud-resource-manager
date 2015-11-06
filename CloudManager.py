#! /usr/bin/env python3
import os
import boto3
from botocore.exceptions import ClientError
from paramiko import *
import argparse
import subprocess

__author__ = 'Thom Hurks'


def ExistingFile(filename):
    if os.path.isfile(filename):
        return filename
    else:
        raise argparse.ArgumentTypeError("%s is not a valid input file!" % filename)


def ParseArgs():
    parser = argparse.ArgumentParser(description='Run the Simple Cloud Manager.')
    parser.add_argument('--pemfile', action='store', required=False, type=ExistingFile, help='The location of the PEM file to use for remote authentication.', metavar='pemfile')
    return parser.parse_args()


def GetInstances(ec2):
    instances = ec2.instances.all()
    instanceCount = 0
    instanceData = dict()
    for instance in instances:
        instanceCount += 1
        stateName = instance.state['Name']
        instanceData[instance.id] = (instance.public_dns_name, instance.state['Name'])
        print("Instance with ID %s has state %s." % (instance.id, stateName))
    print("Discovered a total of %d instances." % instanceCount)
    return instanceData, instanceCount


def CreateNewInstances(ec2, nr, createRealInstance=False):
    succceeded = False
    results = None
    try:
        results = ec2.create_instances(ImageId='ami-daaeaec7', MinCount=nr, MaxCount=nr,
                                       SecurityGroups=['launch-wizard-1'], InstanceType="t2.micro",
                                       Placement={'AvailabilityZone': 'eu-central-1b'},
                                       KeyName='amazon', DryRun=(not createRealInstance))
        if results is not None and len(results) > 0:
            succceeded = True
            for result in results:
                print("Result is: %s" % str(result))
    except ClientError as e:
        errorCode = e.response['Error']['Code']
        if errorCode == 'DryRunOperation':
            succceeded = True
            print("Success!")
        elif errorCode == 'UnauthorizedOperation':
            succceeded = False
            print("Failed!")
    return succceeded, results


def ExecuteRemoteCommand(command, hostname, pemfile, username='ec2-user'):
    try:
        client = SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(AutoAddPolicy())
        client.connect(str(hostname),
                       username=str(username),
                       key_filename=str(pemfile))
        print("Executing command '%s' on host '%s'" % (command, hostname))
        stdin, stdout, stderr = client.exec_command(str(command))
        lines = stdout.read().splitlines()
        for line in lines:
            print(line.decode("utf-8"))
    except (BadHostKeyException, AuthenticationException, SSHException, IOError) as e:
        print("Error connecting to instance with error: %s." % str(e))


def ExecuteLocalCommand(command):
    try:
        completedProcess = subprocess.run(command, shell=True, timeout=None, check=True, stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT, universal_newlines=True, cwd=os.getcwd())
        print("The local process output is: '%s'" % completedProcess.stdout)
    except subprocess.CalledProcessError as e:
        print("The executed command '%s' encountered the error: %s!" % (e.cmd, e.output))
    except subprocess.TimeoutExpired as e:
        print("The executed command '%s' exceeded the timout value: %s!" % (e.cmd, str(e.timeout)))


def CopyFileToRemote(filename, host, pemfile, username='ec2-user'):
    try:
        ExecuteLocalCommand("scp -i %s %s %s@%s:~/ " % (pemfile, filename, username, host))
    except FileNotFoundError:
        print("Couldn't find file '%s' which you wanted to copy!" % filename)


def GetImpairedInstances(ec2_client):
    impairedInstances = []
    statuses = ec2_client.describe_instance_status()['InstanceStatuses']
    for status in statuses:
        if status['SystemStatus'] == 'impaired' or status['InstanceStatus'] == 'impaired':
            impairedInstances.append(status['InstanceId'])
    if len(impairedInstances) > 0:
        print("%d impaired instances found!" % len(impairedInstances))
        return impairedInstances
    else:
        print("All running instances are healthy.")
        return None


def Main():
    args = ParseArgs()
    ec2 = boto3.resource('ec2')
    ec2_client = boto3.client('ec2')
    (succceeded, results) = CreateNewInstances(ec2, 1, False)
    print("CreateNewInstances success: %r. And with results: %s" % (succceeded, results))
    (instanceData, instanceCount) = GetInstances(ec2)
    impairedInstances = GetImpairedInstances(ec2_client)
    print(impairedInstances)
    for key, (dns, status) in instanceData.items():
        if status == 'running':
            CopyFileToRemote("test.txt", dns, args.pemfile)
            ExecuteRemoteCommand("less test.txt", dns, args.pemfile)


if __name__ == "__main__":
    Main()
