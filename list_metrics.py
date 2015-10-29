__author__ = 's138362'

#!/usr/bin/env python
# coding: utf-8

import boto3
import datetime
import pprint

pp = pprint.PrettyPrinter(indent=4)

cloud_watch = boto3.client('cloudwatch', region_name='us-west-2')

list_metrics = cloud_watch.list_metrics()

# pp.pprint(list_metrics)

get_metric_statistics = cloud_watch.get_metric_statistics(
                            Namespace='AWS/EC2',
                            MetricName='CPUUtilization',
                            Dimensions=[
                                {
                                    'Name': 'InstanceId',
                                    'Value': 'i-528e628b'
                                }
                            ],
        StartTime=datetime.datetime.now() - datetime.timedelta(days=1),
        EndTime=datetime.datetime.now(),
        Period=300,
        Statistics=['Average'])

print(get_metric_statistics)