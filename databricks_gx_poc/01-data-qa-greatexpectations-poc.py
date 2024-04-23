# Databricks notebook source
pip install great-expectations

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

# COMMAND ----------

context_root_dir = "/dbfs/FileStore/gx/yellow_tripdata_sample_2019_01.csv"

# COMMAND ----------

context = gx.get_context(context_root_dir=context_root_dir)

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load("dbfs:/FileStore/gx2/yellow_tripdata_2019_01_csv.gz")

# COMMAND ----------

display(df)

# COMMAND ----------

dataframe_datasource = context.sources.add_or_update_spark(
    name="my_spark_in_memory_datasource",
)
csv_file_path = "dbfs:/FileStore/gx2/yellow_tripdata_2019_01_csv.gz"

# COMMAND ----------

df = spark.read.csv(csv_file_path, header=True)
dataframe_asset = dataframe_datasource.add_dataframe_asset(
    name="yellow_tripdata",
    dataframe=df,
)

# COMMAND ----------

batch_request = dataframe_asset.build_batch_request()

# COMMAND ----------

expectation_suite_name = "validate_expectiation_by_biresh"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print(validator.head())

# COMMAND ----------

validator.expect_column_values_to_not_be_null(column="passenger_count")

validator.expect_column_values_to_be_between(
    column="congestion_surcharge", min_value=0, max_value=1000
)

# COMMAND ----------

validator.save_expectation_suite(discard_failed_expectations=False)

# COMMAND ----------

my_checkpoint_name = "my_databricks_checkpoint_biresh"

checkpoint = Checkpoint(
    name=my_checkpoint_name,
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
)

# COMMAND ----------

context.add_or_update_checkpoint(checkpoint=checkpoint)

# COMMAND ----------

checkpoint_result = checkpoint.run()

# COMMAND ----------

#To view the full Checkpoint configuration, run:

print(checkpoint.get_config().to_yaml_str())

# COMMAND ----------

validator.expect_column_values_to_not_be_null("tpep_pickup_datetime")
validator.expect_column_values_to_be_between("passenger_count", min_value=1, max_value=6)
validator.save_expectation_suite()

# COMMAND ----------

context.build_data_docs()

# COMMAND ----------

context.view_validation_result(checkpoint_result)
print(context)
print(validator.head())

# COMMAND ----------

import great_expectations as ge
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.render.renderer import *
from great_expectations.render.view import DefaultJinjaPageView

expectation_suite, validation_result = BasicDatasetProfiler.profile(SparkDFDataset(df))
document_model = ProfilingResultsPageRenderer().render(validation_result)
displayHTML(DefaultJinjaPageView().render(document_model))
