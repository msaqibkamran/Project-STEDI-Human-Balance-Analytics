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
AccelerometerTrusted_node1741828874939 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1741828874939")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1741828913361 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1741828913361")

# Script generated for node Join AT with Step Trainer Trusted
SqlQuery5188 = '''
select stt.*, at.user, at.x, at.y, at.z from stt
join at
on stt.sensorreadingtime = at.timestamp


'''
JoinATwithStepTrainerTrusted_node1741829020013 = sparkSqlQuery(glueContext, query = SqlQuery5188, mapping = {"at":AccelerometerTrusted_node1741828874939, "stt":StepTrainerTrusted_node1741828913361}, transformation_ctx = "JoinATwithStepTrainerTrusted_node1741829020013")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=JoinATwithStepTrainerTrusted_node1741829020013, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1741822688479", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1741829238957 = glueContext.getSink(path="s3://saqib-stedi-lake-house/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1741829238957")
MachineLearningCurated_node1741829238957.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1741829238957.setFormat("json")
MachineLearningCurated_node1741829238957.writeFrame(JoinATwithStepTrainerTrusted_node1741829020013)
job.commit()