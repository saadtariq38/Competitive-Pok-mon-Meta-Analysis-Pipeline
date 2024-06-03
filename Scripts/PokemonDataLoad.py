import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1714468297325 = glueContext.create_dynamic_frame.from_catalog(database="gluedatabase", table_name="run_1714466883594_part_r_00000", transformation_ctx="AmazonS3_node1714468297325")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1714468396324 = glueContext.write_dynamic_frame.from_catalog(frame=AmazonS3_node1714468297325, database="gluedatabase", table_name="run_1714466883594_part_r_00000", transformation_ctx="AWSGlueDataCatalog_node1714468396324")

job.commit()