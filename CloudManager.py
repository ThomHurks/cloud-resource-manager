#! /usr/bin/env python3
import os
import boto3
from botocore.exceptions import ClientError
from paramiko import *
import argparse
import subprocess
import time
import sys
import fileinput
from timeit import default_timer as timer
import re

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
    parser.add_argument('--reboot', action='store_true', required=False, help='Reboot all instances.')
    return parser.parse_args()


def GetInstances(ec2):
    instances = ec2.instances.all()
    instanceCount = 0
    instanceData = dict()
    for instance in instances:
        instanceCount += 1
        stateName = instance.state['Name']
        instanceData[instance.id] = (instance.public_dns_name, stateName, instance)
        print("Instance with ID %s has state %s." % (instance.id, stateName))
    print("Discovered a total of %d instances." % instanceCount)
    return instanceData, instanceCount


def GetRunningHosts(ec2):
    hostnames = []
    (instanceData, instanceCount) = GetInstances(ec2)
    for key, (host, state, instance) in instanceData.items():
        if state == 'running':
            hostnames.append(host)
    return hostnames


def EnsureAllHostsRunning(ec2, ec2_client, waitUntilRunning=False):
    (instanceData, instanceCount) = GetInstances(ec2)
    rebootedInstanceIDs = []
    for key, (host, state, instance) in instanceData.items():
        if state != 'running' and state != 'pending' and state != 'terminated':  # shutting-down | stopping | stopped
            instance.reboot()
            rebootedInstanceIDs.append(instance.id)
            print("Rebooted existing instance: '%s'" % str(instance.id))
    if waitUntilRunning and len(rebootedInstanceIDs) > 0:
        time.sleep(1)
        while True:
            instanceStatuses = ec2_client.describe_instance_status(InstanceIds=rebootedInstanceIDs)['InstanceStatuses']
            for instanceStatus in instanceStatuses:
                curState = instanceStatus['InstanceState']['Name']
                if curState == 'running' or curState == 'terminated':
                    rebootedInstanceIDs.remove(instanceStatus['InstanceId'])
            if len(rebootedInstanceIDs) == 0:
                break
            else:
                time.sleep(5)


def CreateNewInstances(ec2, ec2_client, nr, waitUntilCreated=False, createRealInstance=False):
    succceeded = False
    results = None
    try:
        results = ec2.create_instances(ImageId='ami-91c5d6fd', MinCount=nr, MaxCount=nr,
                                       SecurityGroups=['launch-wizard-1'], InstanceType="t2.micro",
                                       Placement={'AvailabilityZone': 'eu-central-1b'},
                                       KeyName='amazon', DryRun=(not createRealInstance))
        if results is not None and len(results) > 0:
            for newInstance in results:
                print("Created new instance: '%s'" % str(newInstance))
            if waitUntilCreated:
                newInstanceIDs = []
                for newInstance in results:
                    newInstanceIDs.append(newInstance.id)
                time.sleep(1)
                while True:
                    instanceStatuses = ec2_client.describe_instance_status(InstanceIds=newInstanceIDs)['InstanceStatuses']
                    for instanceStatus in instanceStatuses:
                        curState = instanceStatus['InstanceState']['Name']
                        print("State of new instance %s is currently %s" % (instanceStatus['InstanceId'], curState))
                        if curState == 'running':
                            newInstanceIDs.remove(instanceStatus['InstanceId'])
                    if len(instanceStatuses) == 0:
                        break
                    else:
                        time.sleep(5)
            succceeded = True
    except ClientError as e:
        errorCode = e.response['Error']['Code']
        if errorCode == 'DryRunOperation':
            succceeded = True
            print("Success!")
        elif errorCode == 'UnauthorizedOperation':
            succceeded = False
            print("Failed!")
    return succceeded, results


def EnsureEnoughInstances(ec2, ec2_client, nrOfInstances, waitUntilCreated=False, createRealInstances=False):
    runningHosts = GetRunningHosts(ec2)
    extraRequired = nrOfInstances - len(runningHosts)
    if extraRequired > 0:
        (succceeded, results) = CreateNewInstances(ec2, ec2_client, extraRequired, waitUntilCreated, createRealInstances)
        if succceeded:
            if results is not None:
                for result in results:
                    print("Not enough instances. Added instance: '%s'" % str(result))
            else:
                print("No instances actually added. This was probably a dry run.")
        else:
            print("Couldn't ensure the required number of instances!")
    else:
        print("Already enough instances!")


def ExecuteRemoteCommand(command, hostname, pemfile, waitUntilDone=False, username='ec2-user'):
    try:
        client = SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(AutoAddPolicy())
        client.connect(str(hostname),
                       username=str(username),
                       key_filename=str(pemfile))
        print("Executing command '%s' on host '%s'" % (command, hostname))
        if waitUntilDone:
            (stdin, stdout, stderr) = client.exec_command(command)
            lines = stdout.read().splitlines()
            for line in lines:
                print(line.decode("utf-8"))
        else:
            channel = client.get_transport().open_session()
            channel.exec_command(command)
            return client, channel
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
        ExecuteLocalCommand("scp -i %s -o StrictHostKeyChecking=no -C %s %s@%s:~/%s" % (pemfile, filename, username, host, targetfilename))
        print("Copied file %s to host %s." % (filename, host))
    except FileNotFoundError:
        print("Couldn't find file '%s' which you wanted to copy!" % filename)


def CopyFileToLocal(filename, host, pemfile, username='ec2-user', targetfilename="."):
    try:
        ExecuteLocalCommand("scp -i %s %s@%s:~/%s %s" % (pemfile, username, host, filename, targetfilename))
        if os.path.isfile(targetfilename):
            return targetfilename
    except FileNotFoundError:
        print("Couldn't find file '%s' which you wanted to copy!" % filename)


def DistributeFileToHosts(ec2, nrOfInstances, pemfile, filename, hostnames=None):
    if os.path.isfile(filename):
        if hostnames is None or len(hostnames) == 0:
            hostnames = GetRunningHosts(ec2)
        if len(hostnames) < nrOfInstances:
            print("Not enough hosts are running!")
        else:
            targetHosts = []
            print("Copying files to remote hosts...")
            for i in range(0, nrOfInstances):
                host = hostnames[i]
                CopyFileToRemote(filename, host, pemfile)
                targetHosts.append(host)
            return targetHosts
    else:
        print("The file '%s' does not exist!" % filename)


def DistributeSourceVertices(ec2, targetHosts, nrOfInstances, pemfile, vertexfile="sourcevertices.pickle"):
    (filename, extension) = os.path.splitext(vertexfile)
    vertexFiles = []
    for i in range(0, nrOfInstances):
        subfile = filename + "_" + str(i) + extension
        if os.path.isfile(subfile):
            vertexFiles.append(subfile)
    if len(vertexFiles) == nrOfInstances:
        if len(targetHosts) < nrOfInstances:
            print("Not enough hosts are running!")
        else:
            hostToFile = dict()
            print("Copying files to remote hosts...")
            for i, subfile in enumerate(vertexFiles):
                CopyFileToRemote(subfile, targetHosts[i], pemfile, targetfilename=vertexfile)
                hostToFile[targetHosts[i]] = subfile
            return hostToFile
    else:
        print("Could not find all required source vertex files!")


def StartRemoteSSCComputation(command, host, pemfile):
    ExecuteRemoteCommand("rm -f output.txt", host, pemfile, waitUntilDone=True)
    ExecuteRemoteCommand("chmod +x SSC12.py", host, pemfile, waitUntilDone=True)
    (client, channel) = ExecuteRemoteCommand(command, host, pemfile, waitUntilDone=False)
    if channel is not None:
        return client, channel


def PerformComputations(ec2, hostToFile, nrOfInstances, pemfile, graphfile, vertexfile):
    success = True
    if len(hostToFile) >= nrOfInstances:
        command = "./SSC12.py --overwrite compute output.txt preprocessed %s %s" % (graphfile, vertexfile)
        progress = []
        for (host, subfile) in hostToFile.items():
            (client, channel) = StartRemoteSSCComputation(command, host, pemfile)
            if channel is not None:
                progress.append((host, client, channel))
        while True:
            for (host, client, channel) in progress:
                if not channel.exit_status_ready():
                    print("Printing output for host %s:" % host)
                    print(channel.recv(sys.maxsize).decode("utf-8"))
                else:
                    exitCode = channel.recv_exit_status()
                    print("Host %s had exit code %d" % (host, exitCode))
                    progress.remove((host, client, channel))
                    if exitCode != 0:
                        print("Computation encountered an error on host %s!" % host)
                        (client, channel) = StartRemoteSSCComputation(command, host, pemfile)
                        if channel is not None:
                            print("Restarted the failed computation on host %s" % host)
                            progress.append((host, client, channel))
                        else:
                            print("Couldn't restart failed computation.")
                            success = False
                            break
            if len(progress) > 0:
                print("Waiting...")
                time.sleep(1)
            else:
                break
    else:
        success = False
        print("Not enough hosts to start all computations!")
    return success


def GatherResults(targetHosts, filename, outputFilename, pemfile):
    (filename, ext) = os.path.splitext(filename)
    copiedFiles = []
    for i, host in enumerate(targetHosts):
        targetFilename = filename + "_" + str(i) + ext
        copiedFile = CopyFileToLocal(filename + ext, host, pemfile, targetfilename=targetFilename)
        if copiedFile is not None:
            copiedFiles.append(targetFilename)
    if len(copiedFiles) == len(targetHosts):
        print("Copied all files succesfully!")
        re_vertex = re.compile("(\d+)")
        uniqueVertices = set()
        with fileinput.input(files=copiedFiles) as inputfiles:
            for line in inputfiles:
                result = re_vertex.match(line)
                if result is not None:
                    uniqueVertices.add(int(result.group(0)))
        with open(outputFilename, 'w') as output:
            for vertex in sorted(uniqueVertices):
                output.write("%s\n" % str(vertex))
            print("Combined all output files in the file %s" % outputFilename)
    else:
        print("There was a problem copying the files to local!")
        print(copiedFiles)


def RebootInstances(ec2_client, ec2, instance_ids, waitUntilRunning=False):
    (instanceData, instanceCount) = GetInstances(ec2)
    for key, (host, state, instance) in instanceData.items():
        if key in instance_ids:
            instance.reboot()
            print("Rebooted instance %s" % key)
    if waitUntilRunning:
        time.sleep(1)
        while True:
            instanceStatuses= ec2_client.describe_instance_status(InstanceIds=instance_ids)['InstanceStatuses']
            for instanceStatus in instanceStatuses:
                curState = instanceStatus['InstanceState']['Name']
                if curState == 'running' or curState == 'terminated':
                    instance_ids.remove(instanceStatus['InstanceId'])
            if len(instance_ids) == 0:
                break
            else:
                time.sleep(5)
        print("All rebooted instances are now running!")


def RebootAllInstances(ec2_client, ec2, waitUntilRunning=False):
    (instanceData, _) = GetInstances(ec2)
    ids = []
    for key in instanceData.keys():
        ids.append(key)
    print("Rebooting all %d instances." % len(ids))
    RebootInstances(ec2_client, ec2, ids, waitUntilRunning)


def RebootImpairedInstances(ec2_client, ec2, waitUntilRunning=False):
    impairedInstances = []
    statuses = ec2_client.describe_instance_status()['InstanceStatuses']
    for status in statuses:
        if status['SystemStatus']['Status'] == 'impaired' or status['InstanceStatus']['Status'] == 'impaired':
            impairedInstances.append(status['InstanceId'])
    if len(impairedInstances) > 0:
        print("%d impaired instances found!" % len(impairedInstances))
        RebootInstances(ec2_client, ec2, impairedInstances, waitUntilRunning)
    else:
        print("All running instances are healthy.")


def ShowAllRemoteFiles(ec2, pemfile):
    print("Showing the files present on all running instances.")
    (instanceData, instanceCount) = GetInstances(ec2)
    for key, (dns, status, instance) in instanceData.items():
        if status == 'running':
            ExecuteRemoteCommand("ls", dns, pemfile, waitUntilDone=True)


def Main():
    args = ParseArgs()
    ec2 = boto3.resource('ec2')
    ec2_client = boto3.client('ec2')
    if args.reboot:
        RebootAllInstances(ec2_client, ec2, waitUntilRunning=True)
        exit(1)
    EnsureAllHostsRunning(ec2, ec2_client, waitUntilRunning=True)
    RebootImpairedInstances(ec2_client, ec2, waitUntilRunning=True)
    EnsureEnoughInstances(ec2, ec2_client, args.nrofinstances, waitUntilCreated=True, createRealInstances=True)
    ExecuteLocalSSCAlgorithm(args.ssc, args.inputgraph, args.nrofinstances)
    startTime = timer()
    targetHosts = DistributeFileToHosts(ec2, args.nrofinstances, args.pemfile, "graph.pickle")
    if targetHosts is not None:
        hostToFile = DistributeSourceVertices(ec2, targetHosts, args.nrofinstances, args.pemfile)
        if hostToFile is not None:
            DistributeFileToHosts(ec2, args.nrofinstances, args.pemfile, args.ssc, targetHosts)
            endTime = timer()
            preparationTime = endTime - startTime
            startTime = timer()
            if PerformComputations(ec2, hostToFile, args.nrofinstances, args.pemfile, "graph.pickle", "sourcevertices.pickle"):
                print("All computations done succesfully.")
                endTime = timer()
                computingTime = endTime - startTime
                startTime = timer()
                GatherResults(targetHosts, "output.txt", "output.txt", args.pemfile)
                endTime = timer()
                gatherTime = endTime - startTime
                print("Time spent preparing: %s seconds. Time spent computing: %s. Time spent gathering: %s" %
                      (str(preparationTime), str(computingTime), str(gatherTime)))
            else:
                print("Some computation went wrong!")
                exit(1)


if __name__ == "__main__":
    Main()
