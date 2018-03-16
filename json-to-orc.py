import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import SelectFields

# Begin variables to customize with your information
glue_source_database = "gamedata"
glue_source_table = "players_lc"
glue_temp_storage = "s3://blog-example-edz/temp"
glue_relationalize_output_s3_path = "s3://blog-example-edz/output-flat"
# End variables to customize with your information

glueContext = GlueContext(spark.sparkContext)
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = glue_source_database, table_name = glue_source_table, transformation_ctx = "datasource0")
dfc = datasource0.relationalize("root", glue_temp_storage)
blogdata = dfc.select('root')
blogdataoutput = glueContext.write_dynamic_frame.from_options(frame = blogdata, connection_type = "s3", connection_options = {"path": glue_relationalize_output_s3_path}, format = "orc", transformation_ctx = "blogdataoutput")
