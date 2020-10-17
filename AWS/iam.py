import boto3
from botocore.exceptions import ClientError
import logging
import json
import configparser
config = configparser.ConfigParser()
config.read('../dwh.cfg')

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

def create_iam_role(role_name: str, service: str) -> str:
    """Create IAM role for service

    Args:
        role_name (str)
        service (str): service name in lowercase

    Returns:
        str: IAM role ARN
    """
    try:
        iam = boto3.client('iam')
        logging.info("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description = f"Allows {service} to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': f'{service}.amazonaws.com'}}],
                'Version': '2012-10-17'}),
            
        )
        return dwhRole['Role']['Arn']
    except ClientError as err:
        logging.error(err)


def attach_s3_read_only_acces(role_name: str) -> None:
    """Attach se readOnly policy to role_name
    """
    try:
        iam = boto3.client('iam')
        logging.info("Creating a new IAM Role") 
        iam.attach_role_policy(RoleName=role_name,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
    except ClientError as err:
        logging.error(err)


if __name__ == '__main__':
    role_arn = create_iam_role(DWH_IAM_ROLE_NAME, 'redshift')
    attach_s3_read_only_acces(DWH_IAM_ROLE_NAME)