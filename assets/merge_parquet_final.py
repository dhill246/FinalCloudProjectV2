import pandas as pd
import numpy as np
import logging
import io
import boto3
import time
import urllib
import re
import s3fs
from io import BytesIO
import fastparquet
from awsglue.utils import getResolvedOptions
import sys


s3 = boto3.client('s3')
job_args =getResolvedOptions(sys.argv,["my_bucket"])
bucket= job_args["my_bucket"]

source=f"s3://{bucket}/combined_data.parquet"
source2 =f"s3://{bucket}/ticketmaster_new.parquet"

df= pd.read_parquet(source)

df2 = pd.read_parquet(source2)

combined_df = pd.concat([df, df2], ignore_index=True)

print("Combined DataFrame:")
# Print combined DataFrame

print(combined_df)  
combined_df.to_parquet('combined_data.parquet', index=False)
print("DataFrame saved to Parquet file: combined_data.parquet")

with open('combined_data.parquet', 'rb') as file:
    parquet_buffer = BytesIO(file.read())

parquet_buffer.seek(0)

# Upload Parquet file from buffer to S3
s3.put_object(Bucket=bucket, Body=parquet_buffer.getvalue(), Key='combined_data.parquet')

print("Parquet file uploaded to S3: combined_data.parquet")