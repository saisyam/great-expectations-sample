from logging import root
from great_expectations.data_context.types.base import DataContextConfig, FilesystemStoreBackendDefaults
from great_expectations.data_context import BaseDataContext
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd

STORE_FOLDER = "/home/saisyam/work/github/great-expectations-sample/ge_data"
#Setup data config
data_context_config = DataContextConfig(
    config_version = 3,
    plugins_directory = None,
    config_variables_file_path = None,
    datasources = {},
    stores = {
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": STORE_FOLDER+"/expectations"
            }
        },
        "validations_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": STORE_FOLDER+"/validations"
            }
        },
        "checkpoint_store": {
            "class_name": "CheckpointStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": STORE_FOLDER+"/checkpoints"
            }
        },
        "evaluation_parameter_store": {"class_name":"EvaluationParameterStore"}
    },
    expectations_store_name = "expectations_store",
    validations_store_name = "validations_store",
    evaluation_parameter_store_name = "evaluation_parameter_store",
    checkpoint_store_name = "checkpoint_store",
    data_docs_sites={
        "local_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": STORE_FOLDER+"/data_docs",
                
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
        }
    },
    anonymous_usage_statistics={
      "enabled": True
    }
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
suite = context.add_expectation_suite(expectation_suite_name="sales_suite")

expectation_config_1 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "product_group",
        "value_set": ["PG1", "PG2", "PG3", "PG4", "PG5"]
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


