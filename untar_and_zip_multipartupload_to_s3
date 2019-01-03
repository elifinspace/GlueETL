import os
import re
import sys
import json
import gzip
import tarfile
import logging
import boto3
from urlparse import urlparse
from cStringIO import StringIO

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','BucketName','Key','Dest_Bucket','Dest_key'])
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BucketName = args['BucketName']
key = args['Key']
dest_BucketName = args['Dest_Bucket']
dest_key = args['Dest_key']

s3_path = "s3://{}/{}".format(BucketName, key)

localTargetFolder = '/tmp'
regionName = 'eu-west-1'

s3Resource = boto3.resource('s3', region_name = regionName)
localTargetFile = os.path.join(localTargetFolder, key.split("/")[-1])           
s3Resource.Bucket(BucketName).download_file(key, localTargetFile)
 
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__)


client = boto3.client('s3')
main_tar = tarfile.open("{}".format(localTargetFile),'r:gz')

files_in_tar = main_tar.getmembers()
# print(files_in_tar)
# 10.000 max for k  
for member in files_in_tar:
    k=1
    fileobj = main_tar.extractfile(member)
    # large chunk size causes OOM errors : chunk_size = 200 MB
    chunk_size = 200*1048576
    response = client.create_multipart_upload(
        Bucket=dest_BucketName,
        Key=dest_key)
    main_key  = dest_key
    upload_id = response['UploadId']
    print(upload_id)
    Parts = []
    while True:
        data = fileobj.read(chunk_size)
        if not data:
            MultiPartUpload = {'Parts':Parts}
            response3 = client.complete_multipart_upload(
                Bucket = dest_BucketName,
                Key = main_key,
                MultipartUpload = MultiPartUpload,
                UploadId=upload_id
            )
            
            break
        
        # s3.upload_fileobj(Fileobj = StringIO(data), Bucket = dest_BucketName, Key =dest_key+member.name+"_{}".format(k))
          
        response2 = client.upload_part(
            Body=data,
            Bucket=dest_BucketName,
            Key=main_key,
            PartNumber=k,
            UploadId=upload_id
            )
        
        Parts.append({'ETag':response2['ETag'],'PartNumber':k})
        print(Parts)
        k=k+1



job.commit()
