import boto3
import logging
from botocore.exceptions import ClientError
import os
import glob

def create_bucket(bucket_name: str, region=None) -> bool:
    """Create s3 bucket in a specified region
    
    Args:
        bucket_name (str)
        region (str, optinal)
    Return:
        bool: True-> created, False-> not created
    """
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            print(location)
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as err:
        logging.error(err)
        return False
    return True

def block_public_acces(bucket_name: str) -> None:
    """Block all bucket public access

    Args:
        bucket_name(str)
    Returns:
        None
    """
    s3_client = boto3.client('s3')
    s3_client.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        }
)


def upload_file(file_name: str, bucket: str, object_name=None) -> bool:
    """Upload a file to an S3 bucket

    Args:
        file_name: File to upload
        bucket: Bucket to upload to
        object_name: S3 object name. If not specified then file_name is used
    Returns:
        bool: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_files_path(file_path: str) -> list:
    """Get all files path

    Args:
        file_path: root folder path
    Returns:
        list: list of string containing all files paths
    """
    filepath='data'
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(f)
    return all_files


if __name__ == '__main__':

    bucket_name = 'sparkify-db'

    # Creating Bucket
    create_bucket(bucket_name)

    # Blocking public access
    block_public_acces(bucket_name)

    # Uploading files
    all_files = get_files_path('data')
    for file in all_files:
        upload_file(file, bucket_name)