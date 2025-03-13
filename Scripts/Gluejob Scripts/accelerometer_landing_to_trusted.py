import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1741815538779 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1741815538779")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1741814950616 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1741814950616")

# Script generated for node Join only Customer Trusted
SqlQuery4841 = '''
SELECT 
    al.user,
    al.timestamp,
    al.x,
    al.y,
    al.z
FROM al
INNER JOIN ct
ON al.user = ct.email;
'''
JoinonlyCustomerTrusted_node1741815356124 = sparkSqlQuery(glueContext, query = SqlQuery4841, mapping = {"al":AccelerometerLanding_node1741814950616, "ct":CustomerTrusted_node1741815538779}, transformation_ctx = "JoinonlyCustomerTrusted_node1741815356124")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=JoinonlyCustomerTrusted_node1741815356124, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741812950876", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1741816497160 = glueContext.getSink(path="s3://saqib-stedi-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1741816497160")
AccelerometerTrusted_node1741816497160.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1741816497160.setFormat("json")
AccelerometerTrusted_node1741816497160.writeFrame(JoinonlyCustomerTrusted_node1741815356124)
job.commit()