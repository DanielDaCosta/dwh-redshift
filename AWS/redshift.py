import boto3
from botocore.exceptions import ClientError
import logging
import json
import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

def create_redshift_cluster(role_arn: str) -> dict:
    """Creates Redshift Cluster based on DWH config file
    with roleArn permission

    Args:
        role_arn (str): redshift iam role arn
    Returns:
        dict: redshift response
    """
    try:
        redshift = boto3.client('redshift')
        response = redshift.create_cluster(
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[role_arn]
        )
        return response
    except Exception as err:
        logging.error(err)

def open_incoming_tcp(vpc_id: str):
    """Open VPC TCP connection

    Args:
        vpc_id (str)
    """
    try:
        ec2 = boto3.resource('ec2')
        vpc = ec2.Vpc(id=vpc_id)
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as err:
        logging.error(err)

if __name__ == '__main__':
    iam = boto3.client('iam')
    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    response = create_redshift_cluster(role_arn)

    ## Check redshift creation
    redshift = boto3.client('redshift')
    response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    if response['ClusterStatus'] == 'available':
        DWH_ENDPOINT = response['Endpoint']['Address']
        DWH_ROLE_ARN = role_arn
        VPC_ID = response['VpcId']
        open_incoming_tcp(VPC_ID)
