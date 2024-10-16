from typing import Collection
from typing import Dict
from typing import List


def generate_spark_step(
    job_name: str,
    util_zip_folder: str,
    script_loc: str,
    params: List[str],
    executor_memory: str = "4G",
) -> List[Dict]:
    """Generate steps to be used with an Airflow EmrAddStepsOperator."""
    return [
        {
            "Name": job_name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--master",
                    "yarn",
                    "--deploy-mode",
                    "client",
                    "--name",
                    job_name,
                    "--executor-memory",
                    executor_memory,
                    "--executor-cores",
                    "4",
                    "--driver-memory",
                    "32G",
                    "--conf",
                    "spark.driver.maxResultSize=24g",
                    "--conf",
                    "spark.driver.memoryOverhead=2458m",
                    "--conf",
                    "spark.executor.memoryOverhead=2458m",
                    "--conf",
                    "spark.sql.shuffle.partitions=15000",
                    "--py-files",
                    util_zip_folder,
                    script_loc,
                    *params,
                ],
            },
        }
    ]


def bid_price_log_aggregate_config(env: str):  # type: ignore[no-untyped-def]
    JOB_FLOW_OVERRIDES = {
        "Name": "Aggregate Bid price logs minutes",
        "LogUri": "s3n://aws-logs-077854988703-us-west-2/elasticmapreduce/",
        "ReleaseLabel": "emr-7.1.0",
        "Instances": {
            "Ec2SubnetId": "subnet-04e00fc39d3b8da34",
            "EmrManagedMasterSecurityGroup": "sg-04126da0a1508dd0f",
            "EmrManagedSlaveSecurityGroup": "sg-0df82ffb5aa81bc05",
            "Ec2KeyName": "team-bidder",
            "AdditionalMasterSecurityGroups": [],
            "AdditionalSlaveSecurityGroups": [],
            "ServiceAccessSecurityGroup": "sg-0efd02f107220014a",
            "InstanceFleets": [
                {
                    "Name": "Primary",
                    "InstanceFleetType": "MASTER",
                    "TargetSpotCapacity": 0,
                    "TargetOnDemandCapacity": 1,
                    "InstanceTypeConfigs": [
                        {
                            "WeightedCapacity": 1,
                            "EbsConfiguration": {
                                "EbsBlockDeviceConfigs": [
                                    {"VolumeSpecification": {"VolumeType": "gp2", "SizeInGB": 64}},
                                    {"VolumeSpecification": {"VolumeType": "gp2", "SizeInGB": 64}},
                                ]
                            },
                            "BidPriceAsPercentageOfOnDemandPrice": 100,
                            "InstanceType": "m7g.4xlarge",
                        },
                    ],
                },
                {
                    "Name": "Core",
                    "InstanceFleetType": "CORE",
                    "TargetSpotCapacity": 0,
                    "TargetOnDemandCapacity": 256,
                    "InstanceTypeConfigs": [
                        {
                            "WeightedCapacity": 8,
                            "EbsConfiguration": {
                                "EbsBlockDeviceConfigs": [
                                    {
                                        "VolumeSpecification": {
                                            "VolumeType": "gp3",
                                            "Iops": 3000,
                                            "SizeInGB": 512,
                                            "Throughput": 125,
                                        }
                                    },
                                    {
                                        "VolumeSpecification": {
                                            "VolumeType": "gp3",
                                            "Iops": 3000,
                                            "SizeInGB": 512,
                                            "Throughput": 125,
                                        }
                                    },
                                ]
                            },
                            "BidPriceAsPercentageOfOnDemandPrice": 100,
                            "InstanceType": "r7g.8xlarge",
                        },
                    ],
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        "Applications": [
            {"Name": "AmazonCloudWatchAgent"},
            {"Name": "Hadoop"},
            {"Name": "Hive"},
            {"Name": "Spark"},
            {"Name": "Livy"},
            {"Name": "JupyterHub"},
            {"Name": "JupyterEnterpriseGateway"},
        ],
        "Tags": [
            {"Key": "application", "Value": "bid_log_aggregation"},
            {"Key": "environment", "Value": f"{env}"},
            {"Key": "team", "Value": "bidder"},
        ],
        "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
        "ServiceRole": "LogImporter_Emr_Role_Dev",
        "JobFlowRole": "LogImporter_Emr_EC2_Role_Dev",
        "VisibleToAllUsers": True,
        "Configurations": [
            {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}},
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.jars": f"s3://mntn-data-archive-{env}/_spark/drivers/redshift-jdbc42-2.1.0.30.jar,\
                    s3://mntn-data-archive-{env}/_spark/drivers/aws-java-sdk-bundle-1.11.874.jar,\
                    s3://mntn-data-archive-{env}/_spark/drivers/hadoop-aws-3.2.2.jar"
                },
            }
            # ,
            # {
            #     "Classification": "hive-site",
            #     "Properties": {
            #         "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            #         # noqa: B950
            #     },
            # },
            # {
            #     "Classification": "spark-hive-site",
            #     "Properties": {
            #         "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            #         # noqa: B950
            #     },
            # },
        ],
        "BootstrapActions": [
            {
                "Name": "bidder logs aggregation bootstrap",
                "ScriptBootstrapAction": {
                    "Path": f"s3://bidder-emr-dev/pyspark/bootstrap.sh",
                    "Args": [f"{env}"],
                },
            }
        ],
    }

    return JOB_FLOW_OVERRIDES


def bid_price_logs_agg(
    data_source_id: int, env: str, date: str
) -> list[dict[str, Collection[str]]]:
    spark_steps = [
        {
            "Name": f"aggregate bid logs for  {date} in {env} for ds id {data_source_id}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "client",
                    "--master",
                    "yarn",
                    "spark.sql.optimizer.dynamicPartitionPruning.enabled=true",
                    "--conf",
                    "spark.driver.maxResultSize=32G",
                    "--conf",
                    "spark.sql.shuffle.partitions=15000",
                    "--conf",
                    "spark.driver.memoryOverhead=3277m",
                    "--conf",
                    "spark.executor.memoryOverhead=3277m",
                    "--conf",
                    "spark.executor.instances=512",
                    "--conf",
                    "spark.num.executors=512",
                    "--conf",
                    "spark.executor.memory=32G",
                    "--conf",
                    "spark.executor.cores=5",
                    "--conf",
                    "spark.driver.cores=5",
                    "--conf",
                    "spark.driver.memory=32G",
                    "--conf",
                    "spark.memory.fraction=0.8",
                    "--conf",
                    "spark.speculation=true",
                    "--conf",
                    "spark.dynamicAllocation.enabled=false",
                    "--conf",
                    "spark.yarn.heterogeneousExecutors.enabled=false",
                    "--conf",
                    "spark.emr.default.executor.cores=5",
                    "--conf",
                    "spark.emr.default.executor.memory=32G",
                    f"s3://mntn-data-airflow-{env}/dags/targeting/jobs/hashed_email_signals.py",
                    "-d",  # param switch for date in "YYYY-MM-DD"
                    f"{date}",
                    "-e",
                    f"{env}",
                    "-i",
                    f"{data_source_id}",
                ],
            },
        }
    ]

    # date = '2024-10-10'
    # hour = '12'
    # output_location = s3a: // mntn - data - archive - dev / bid_price_log_agg
    #

    return spark_steps