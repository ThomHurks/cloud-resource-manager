__author__ = 'Thom Hurks and Sander Kools'
import boto3
from subprocess import call


ec2 = boto3.resource('ec2')
s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')

print('get instances')
instances = ec2.instances.all()


for instance in instances:
    instanceId = instance.id
    instancepublicDNS = instance.public_dns_name



locationToKey = "D:\Documents\Github\gridAndCloud\key\Grabot.pem"
scriptToRun = "python test.py"
uploadScript = "test.py"
uploadFile = "triangle.txt"

call(["scp", "-i", locationToKey, uploadFile, "ec2-user@"+instancepublicDNS+":"+uploadFile])
call(["scp", "-i", locationToKey, uploadScript, "ec2-user@"+instancepublicDNS+":"+uploadScript])
call(["ssh", "-i", locationToKey, "ec2-user@"+instancepublicDNS, scriptToRun])

# scp -i D:\Documents\Github\gridAndCloud\key\Grabot.pem *file* ec2-user@ec2-52-26-179-116.us-west-2.compute.amazonaws.com:*file*
# call(["ssh", "-i", "D:\Documents\Github\gridAndCloud\key\Grabot.pem", "ec2-user@ec2-52-26-179-116.us-west-2.compute.amazonaws.com", "python test.py"])
