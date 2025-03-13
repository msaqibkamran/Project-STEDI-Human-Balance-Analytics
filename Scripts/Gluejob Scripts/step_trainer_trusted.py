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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1741827767759 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://saqib-stedi-lake-house/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1741827767759")

# Script generated for node Customers Curated
CustomersCurated_node1741827676366 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="CustomersCurated_node1741827676366")

# Script generated for node Join Customer Step Trainer Landing
SqlQuery5330 = '''
select cc.*, stl.sensorReadingTime, stl.distanceFromObject from stl
join cc
on stl.serialnumber = cc.serialnumber
'''
JoinCustomerStepTrainerLanding_node1741827857692 = sparkSqlQuery(glueContext, query = SqlQuery5330, mapping = {"stl":StepTrainerLanding_node1741827767759, "cc":CustomersCurated_node1741827676366}, transformation_ctx = "JoinCustomerStepTrainerLanding_node1741827857692")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=JoinCustomerStepTrainerLanding_node1741827857692, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741822688479", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1741828103085 = glueContext.getSink(path="s3://saqib-stedi-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1741828103085")
StepTrainerTrusted_node1741828103085.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1741828103085.setFormat("json")
StepTrainerTrusted_node1741828103085.writeFrame(JoinCustomerStepTrainerLanding_node1741827857692)
job.commit()