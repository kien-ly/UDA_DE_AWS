import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_landing
customer_landing_node1697790000553 = glueContext.create_dynamic_frame.from_catalog(
    database="project",
    table_name="customer_landing",
    transformation_ctx="customer_landing_node1697790000553",
)

# Script generated for node Filter
Filter_node1697708928671 = Filter.apply(
    frame=customer_landing_node1697790000553,
    f=lambda row: (row["sharewithresearchasofdate"] > 0),
    transformation_ctx="Filter_node1697708928671",
)

# Script generated for node customer_trusted
customer_trusted_node1697792207923 = glueContext.getSink(
    path="s3://udacity-glue-spark-bucket/project/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1697792207923",
)
customer_trusted_node1697792207923.setCatalogInfo(
    catalogDatabase="project", catalogTableName="customer_trusted"
)
customer_trusted_node1697792207923.setFormat("json")
customer_trusted_node1697792207923.writeFrame(Filter_node1697708928671)
job.commit()
