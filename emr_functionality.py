import boto3
import configparser
import argparse
import pandas as pd
import json
import os
import subprocess

credential_config = configparser.ConfigParser()
config = configparser.ConfigParser()

credential_config.read('dl.cfg')
config.read('emr.cfg')

CFG = {
    'KEY': credential_config.get('AWS', 'AWS_ACCESS_KEY_ID'),
    'SECRET': credential_config.get('AWS', 'AWS_SECRET_ACCESS_KEY'),
    'REGION': credential_config.get('AWS', 'REGION'),
    'BUCKET': credential_config.get('AWS', 'BUCKET_NAME'),

    'MASTER_TYPE': config.get('EMR', 'MASTER_TYPE'),
    'MASTER_INSTANCE_COUNT': config.get('EMR', 'MASTER_INSTANCE_COUNT'),
    'SLAVE_TYPE': config.get('EMR', 'SLAVE_TYPE'),
    'SLAVE_INSTANCE_COUNT': config.get('EMR', 'SLAVE_INSTANCE_COUNT'),
    'EC2_KEY_NAME': config.get('EMR', 'EC2_KEY_NAME'),
}


def list_clusters_steps(clusterId=None, StepStates=['RUNNING', 'PENDING']):
    ''' 
    List the steps assocaited with the clusterId and in the state specified in stages. By default will list the RUNNING steps in the first active EMR cluster
    
    Input:
    1. clusterId - cluster ID of the created cluster
    2. StepStates - List of all step states. see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.list_steps
    '''
    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    # Get an active clusterId if none was provided.
    if clusterId is None:
        clusters = emr.list_clusters()

        active_clusters = [c["Id"] for c in clusters["Clusters"] 
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]

        clusterId = active_clusters[0]

    # List all steps
    response = emr.list_steps(
                    ClusterId=clusterId,
                    StepStates=StepStates
                )

    for step in response["Steps"]:
        print (f"{step['Status']['State']} -- {step['Id']} -- {step['Name']} -- { ' '.join(step['Config']['Args']) }")
        
def copy_etl_to_master(ClusterId=None):
    '''
    Copy the ETL to the MASTER node of the EMR cluster described by 
        
    Input:
    ClusterId - If ClusterId is None copy to the first active cluster. This is done using SCP and assumes it is properly setup beforehand.
    '''

    print ("=== Copy ETL file to Master node")

    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    if ClusterId is None:
        clusters = emr.list_clusters()

        active_clusters = [c["Id"] for c in clusters["Clusters"] 
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]

        ClusterId = active_clusters[0]

    # Get the DNS address for the Master Node
    response = emr.list_instances(
            ClusterId=ClusterId,
            InstanceGroupTypes=['MASTER'],
            InstanceStates=['RUNNING'],
        )
    if response['Instances']:
        master_dns = response['Instances'][0]["PublicDnsName"]
        print ("MASTER DNS", master_dns)
    else:
        print("No Running Master")

    # SCP the etl.py file
    if master_dns:
        URL = f"hadoop@{master_dns}:/tmp/"
        if os.path.exists("etl.py"):
            subprocess.run(["scp", "etl.py", URL], capture_output=True)
        else:
            print("No etl.py file")
            
            
            
def describe_emr_clusters():
    '''
    This function describes the cluster which is already created.
    '''
    print ("=== Describe EMR Clusters")

    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    clusters = emr.list_clusters()

    if clusters:
        res = pd.DataFrame(clusters['Clusters']).to_json(indent=4)
        print(res)
    else:
        print("Error no cluster information")

    active_clusters = [c["Id"] for c in clusters["Clusters"] 
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]

    response = emr.list_instances(
            ClusterId=active_clusters[0],
            InstanceGroupTypes=['MASTER'],
            InstanceStates=['RUNNING'],
        )
    # print(response)

    if response['Instances']:
        res = pd.DataFrame(response['Instances']).to_json(indent=4)
        print (res)
    else:
        print("No Running Master")
        
        
    
def create_emr_cluster():   
    '''
    This function is responsible for creating AWS EMR cluster using the provided AWS credentials and EMR details.
    '''
    print ("=== Create EMR Cluster")

    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    step_args = ["/usr/bin/spark-submit", "/tmp/etl.py", "--mode", "emr"]

    cluster_id = emr.run_job_flow(
        Name='spark-EMR',
        LogUri='s3://'+CFG["BUCKET"]+'/logs',
        ReleaseLabel='emr-5.20.0',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': CFG['MASTER_TYPE'],
                    'InstanceCount': int(CFG['MASTER_INSTANCE_COUNT']),
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': CFG['SLAVE_TYPE'],
                    'InstanceCount': int(CFG['SLAVE_INSTANCE_COUNT']),
                }
            ],
            'Ec2KeyName': CFG['EC2_KEY_NAME'],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-27ba277f',
        },
        Steps=[
                {
                'Name': 'songplay-etl',
                'ActionOnFailure': 'CANCEL_AND_WAIT',            
                'HadoopJarStep': {
                    'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': step_args
                }
            },

        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    )

    print(cluster_id)
    
    
def submit_job():
    '''
    Create a step Job and submitted to any available EMR cluster
    '''
    print ("=== Submit ETL job")

    emr = boto3.client('emr',
                          aws_access_key_id=CFG["KEY"],
                          aws_secret_access_key=CFG["SECRET"],
                          region_name=CFG["REGION"]
                        )

    clusters = emr.list_clusters()

    # choose a cluster that is available
    clusters = [c["Id"] for c in clusters["Clusters"] 
                if c["Status"]["State"] in ["RUNNING", "WAITING"]]

    if not clusters:
        raise Exception("No valid clusters")

    # take the first relevant cluster
    cluster_id = clusters[0]
    step_args = ["/usr/bin/spark-submit", "/tmp/etl.py", "--mode", "emr"]

    action = emr.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[
            {
                'Name': 'songplay-etl',
                'ActionOnFailure': 'CANCEL_AND_WAIT',            
                'HadoopJarStep': {
                    'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                    'Args': step_args
                    }
            },
            ]
        )
    print(action)
    
    
def argparser():
    ''' 
    Command Line parser for the script
    '''

    parser = argparse.ArgumentParser(description='Management utility for EMR cluster')
    parser.add_argument('--cmd', 
                        type=str,
                        required=True,
                        choices=["create-cluster", 
                                 "submit-job", 
                                 "describe-clusters", 
                                 "setup-etl",
                                 "list_clusters_steps"]
                        )

    args = parser.parse_args()

    return args
    
    
def main():
    ''' 
    Get a running EMR cluster and submit the etl.py spark job. The etl.py file must have already been copied to the master node
    '''
    
    args = argparser()
    cmd = args.cmd

    if cmd == "submit-job":
        copy_etl_to_master()
        submit_job()
    elif cmd == "create-cluster":
        create_emr_cluster()
    elif cmd== "describe-clusters":
        describe_emr_clusters()
    elif cmd == "setup-etl":
        copy_etl_to_master()
    elif cmd == "list_clusters_steps":
        list_clusters_steps()

if __name__ == "__main__":
    main()