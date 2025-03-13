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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1741825758479 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1741825758479")

# Script generated for node Customer Trusted
CustomerTrusted_node1741826474344 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1741826474344")

# Script generated for node Remove Duplicates
SqlQuery5052 = '''
select distinct serialnumber, birthDay, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithfriendsasofdate, sharewithpublicasofdate
from ct
JOIN at
ON ct.email = at.user;

'''
RemoveDuplicates_node1741825929693 = sparkSqlQuery(glueContext, query = SqlQuery5052, mapping = {"at":AccelerometerTrusted_node1741825758479, "ct":CustomerTrusted_node1741826474344}, transformation_ctx = "RemoveDuplicates_node1741825929693")

# Script generated for node Save Curated Customers
EvaluateDataQuality().process_rows(frame=RemoveDuplicates_node1741825929693, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741822688479", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
SaveCuratedCustomers_node1741826249818 = glueContext.getSink(path="s3://saqib-stedi-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="SaveCuratedCustomers_node1741826249818")
SaveCuratedCustomers_node1741826249818.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
SaveCuratedCustomers_node1741826249818.setFormat("json")
SaveCuratedCustomers_node1741826249818.writeFrame(RemoveDuplicates_node1741825929693)
job.commit()