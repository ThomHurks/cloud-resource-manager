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
    parser.add_argument('--ssc', action='store', required=False, type=ExistingFile, help='The location of the SSC algorithm.', metavar='ssc')
    parser.add_argument('--nrofinstances', action='store', required=False, type=int, help='The nr of instances that should be launched.', metavar='nrofinstances')
    parser.add_argument('--inputgraph', action='store', required=False, type=ExistingFile, help='The input graph in text format.', metavar='inputgraph')

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


def GetRunningHosts(ec2):
    hostnames = []
    (instanceData, instanceCount) = GetInstances(ec2)
    for key, (host, state) in instanceData.items():
        if state == 'running':
            hostnames.append(host)
    return hostnames


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


def ExecuteLocalSSCAlgorithm(SSC_program, input_graph, nrOfInstances):
    ExecuteLocalCommand("%s --overwrite preprocess %s graph.pickle sourcevertices.pickle --nrofvertexfiles %d" %
                        (SSC_program, input_graph, nrOfInstances))


def CopyFileToRemote(filename, host, pemfile, username='ec2-user', targetfilename=""):
    try:
        ExecuteLocalCommand("scp -i %s %s %s@%s:~/%s" % (pemfile, filename, username, host, targetfilename))
    except FileNotFoundError:
        print("Couldn't find file '%s' which you wanted to copy!" % filename)


def DistributeFileToHosts(ec2, nrOfInstances, pemfile, filename):
    if os.path.isfile(filename):
        hostnames = GetRunningHosts(ec2)
        if len(hostnames) < nrOfInstances:
            print("Not enough hosts are running!")
        else:
            for host in hostnames:
                CopyFileToRemote(filename, host, pemfile)
    else:
        print("The file '%s' does not exist!" % filename)


def DistributeSourceVertices(ec2, nrOfInstances, pemfile, vertexfile="sourcevertices.pickle"):
    (filename, extension) = os.path.splitext(vertexfile)
    vertexFiles = []
    for i in range(0, nrOfInstances):
        subfile = filename + "_" + str(i) + extension
        if os.path.isfile(subfile):
            vertexFiles.append(subfile)
    if len(vertexFiles) == nrOfInstances:
        hostnames = GetRunningHosts(ec2)
        if len(hostnames) < nrOfInstances:
            print("Not enough hosts are running!")
        else:
            for i, subfile in enumerate(vertexFiles):
                CopyFileToRemote(subfile, hostnames[i], pemfile, targetfilename=vertexfile)
    else:
        print("Could not find all required source vertex files!")


def StartComputations(ec2, nrOfInstances, pemfile, graphfile, vertexfile):
    hostnames = GetRunningHosts(ec2)
    if len(hostnames) >= nrOfInstances:
        command = "./SSC12.py --overwrite compute output.txt preprocessed %s %s" % (graphfile, vertexfile)
        for host in hostnames:
            ExecuteRemoteCommand("chmod +x SSC12.py", host, pemfile)
            ExecuteRemoteCommand(command, host, pemfile)
    else:
        print("Not enough hosts to start all computations!")



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
    ExecuteLocalSSCAlgorithm(args.ssc, args.inputgraph, args.nrofinstances)
    DistributeFileToHosts(ec2, args.nrofinstances, args.pemfile, "graph.pickle")
    DistributeSourceVertices(ec2, args.nrofinstances, args.pemfile)
    DistributeFileToHosts(ec2, args.nrofinstances, args.pemfile, args.ssc)
    StartComputations(ec2, args.nrofinstances, args.pemfile, "graph.pickle", "sourcevertices.pickle")
    for key, (dns, status) in instanceData.items():
        if status == 'running':
            ExecuteRemoteCommand("ls", dns, args.pemfile)


if __name__ == "__main__":
    Main()
