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

# Script generated for node team-data-table
teamdatatable_node1714635110533 = glueContext.create_dynamic_frame.from_catalog(database="gluedatabase", table_name="team_data_bucket", transformation_ctx="teamdatatable_node1714635110533")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1714635138659 = glueContext.write_dynamic_frame.from_catalog(frame=teamdatatable_node1714635110533, database="gluedatabase", table_name="team_data_bucket", transformation_ctx="AWSGlueDataCatalog_node1714635138659")

job.commit()