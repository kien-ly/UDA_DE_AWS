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

# Script generated for node accelerometer_landing
accelerometer_landing_node1685352066932 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1685352066932",
)

# Script generated for node customer_landing
customer_landing_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="customer_landing",
    transformation_ctx="customer_landing_node1",
)

# Script generated for node Join
Join_node1685352536679 = Join.apply(
    frame1=accelerometer_landing_node1685352066932,
    frame2=customer_landing_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1685352536679",
)

# Script generated for node Drop Fields
DropFields_node1697648744694 = DropFields.apply(
    frame=Join_node1685352536679,
    paths=[
        "email",
        "customername",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1697648744694",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1697816227119 = glueContext.getSink(
    path="s3://udacity-glue-spark-bucket/project/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1697816227119",
)
accelerometer_trusted_node1697816227119.setCatalogInfo(
    catalogDatabase="project", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1697816227119.setFormat("json")
accelerometer_trusted_node1697816227119.writeFrame(DropFields_node1697648744694)
job.commit()
