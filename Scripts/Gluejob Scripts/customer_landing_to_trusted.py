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

# Script generated for node Customer Landing
CustomerLanding_node1741810223327 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://saqib-stedi-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1741810223327")

# Script generated for node Filter by Opt In
SqlQuery4167 = '''
select * from customer_landing where shareWithResearchAsOfDate is not null;

'''
FilterbyOptIn_node1741810597418 = sparkSqlQuery(glueContext, query = SqlQuery4167, mapping = {"customer_landing":CustomerLanding_node1741810223327}, transformation_ctx = "FilterbyOptIn_node1741810597418")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=FilterbyOptIn_node1741810597418, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741810100113", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1741811395745 = glueContext.getSink(path="s3://saqib-stedi-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1741811395745")
CustomerTrusted_node1741811395745.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1741811395745.setFormat("json")
CustomerTrusted_node1741811395745.writeFrame(FilterbyOptIn_node1741810597418)
job.commit()