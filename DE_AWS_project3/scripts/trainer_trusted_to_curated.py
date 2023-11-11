import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1697791677576 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1697791677576",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1697791722685 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1697791722685",
)

# Script generated for node Join
Join_node1697791740134 = Join.apply(
    frame1=step_trainer_trusted_node1697791722685,
    frame2=accelerometer_trusted_node1697791677576,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1697791740134",
)

# Script generated for node Amazon S3
AmazonS3_node1697791977285 = glueContext.getSink(
    path="s3://udacity-glue-spark-bucket/project/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1697791977285",
)
AmazonS3_node1697791977285.setCatalogInfo(
    catalogDatabase="project", catalogTableName="machine_learning_curated"
)
AmazonS3_node1697791977285.setFormat("json")
AmazonS3_node1697791977285.writeFrame(DropFields_node1697791832615)
job.commit()
