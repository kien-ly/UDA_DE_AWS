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

# Script generated for node customer_curated
customer_curated_node1697789624653 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1697789624653",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1697789640534 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1697789640534",
)

# Script generated for node Join
Join_node1697789673496 = Join.apply(
    frame1=step_trainer_landing_node1697789640534,
    frame2=customer_curated_node1697789624653,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1697789673496",
)

# Script generated for node Drop Fields
DropFields_node1697789702414 = DropFields.apply(
    frame=Join_node1697789673496,
    paths=[
        "phone",
        "email",
        "customername",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "lastupdatedate",
        "birthday",
        "`.serialnumber`",
        "registrationdate",
    ],
    transformation_ctx="DropFields_node1697789702414",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1697815469018 = glueContext.getSink(
    path="s3://udacity-glue-spark-bucket/project/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1697815469018",
)
step_trainer_trusted_node1697815469018.setCatalogInfo(
    catalogDatabase="project", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1697815469018.setFormat("json")
step_trainer_trusted_node1697815469018.writeFrame(DropFields_node1697789702414)
job.commit()
