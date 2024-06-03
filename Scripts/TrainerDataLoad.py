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

# Script generated for node TrainerData
TrainerData_node1714636141976 = glueContext.create_dynamic_frame.from_catalog(database="gluedatabase", table_name="trainercleaneddata", transformation_ctx="TrainerData_node1714636141976")

# Script generated for node TrainerCatalogTable
TrainerCatalogTable_node1714636158405 = glueContext.write_dynamic_frame.from_catalog(frame=TrainerData_node1714636141976, database="gluedatabase", table_name="trainercleaneddata", transformation_ctx="TrainerCatalogTable_node1714636158405")

job.commit()