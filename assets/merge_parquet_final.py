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
from awsglue.utils import getResolvedOptions
import sys


s3 = boto3.client('s3')
job_args =getResolvedOptions(sys.argv,["my_bucket"])
bucket= job_args["my_bucket"]

source=f"s3://{bucket}/combined_data.parquet"
source2 =f"s3://{bucket}/ticketmaster_new.parquet"

df2 = pd.read_parquet(source2)

try: 
    df= pd.read_parquet(source)
    combined_df = pd.concat([df, df2], ignore_index=True)

    print("Combined DataFrame:")
    # Print combined DataFrame

    print(combined_df)  
    combined_df.to_parquet(source, index=False)
    print("DataFrame saved to Parquet file: combined_data.parquet")

except FileNotFoundError:

    df2.to_parquet(source, index=False)
