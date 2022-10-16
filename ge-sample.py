from logging import root
from great_expectations.data_context.types.base import DataContextConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context import BaseDataContext
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd

#Setup data config
data_context_config  = DataContextConfig(
    datasources = {},
    store_backend_defaults = FilesystemStoreBackendDefaults(root_directory="/Users/saisyam/work/github/great-expectations-sample/ge_data")
)

context = BaseDataContext(project_config = data_context_config)

# Setup datasource config
datasource_config = {
    "name": "sales_datasource",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "module_name": "great_expectations.datasource.data_connector",
            "batch_identifiers": ["default_identifier_name"],
        },
    },
}
context.add_datasource(**datasource_config)

# Create expectations suite and add expectations
suite = context.create_expectation_suite(expectation_suite_name="sales_suite", overwrite_existing=True)

expectation_config_1 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "product_group",
        "value_set": ["PG1", "PG2", "PG3", "PG4", "PG5", "PG6"]
    }
) 
suite.add_expectation(expectation_configuration=expectation_config_1)

expectation_config_2 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_unique",
    kwargs={
        "column": "id"
    }
) 
suite.add_expectation(expectation_configuration=expectation_config_2)
context.save_expectation_suite(suite, "sales_suite")

# load and validate data
df = pd.read_csv("./sales.csv")

batch_request = RuntimeBatchRequest(
    datasource_name="sales_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="product_sales",
    runtime_parameters={"batch_data":df},
    batch_identifiers={"default_identifier_name":"default_identifier"}
)

checkpoint_config = {
    "name": "product_sales_checkpoint",
    "config_version": 1,
    "class_name":"SimpleCheckpoint",
    "expectation_suite_name": "sales_suite"
}
context.add_checkpoint(**checkpoint_config)
results = context.run_checkpoint(
    checkpoint_name="product_sales_checkpoint",
    validations=[
        {"batch_request": batch_request}
    ]
)
print(results)


