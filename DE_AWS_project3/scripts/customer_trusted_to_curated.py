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
accelerometer_trusted_node1697791127273 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1697791127273",
)

# Script generated for node customer_trusted
customer_trusted_node1697791148601 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1697791148601",
)

# Script generated for node Join
Join_node1697789231276 = Join.apply(
    frame1=accelerometer_trusted_node1697791127273,
    frame2=customer_trusted_node1697791148601,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1697789231276",
)

# Script generated for node Drop Fields
DropFields_node1697789253454 = DropFields.apply(
    frame=Join_node1697789231276,
    paths=["y", "user", "timestamp", "x", "z"],
    transformation_ctx="DropFields_node1697789253454",
)

# Script generated for node customer_curated
customer_curated_node1697816036470 = glueContext.getSink(
    path="s3://udacity-glue-spark-bucket/project/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1697816036470",
)
customer_curated_node1697816036470.setCatalogInfo(
    catalogDatabase="project", catalogTableName="customer_curated"
)
customer_curated_node1697816036470.setFormat("json")
customer_curated_node1697816036470.writeFrame(DropFields_node1697789253454)
job.commit()
