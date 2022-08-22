import os
import sys
sys.path.append('/app/src/data_quality')

from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.resource_identifiers import \
    ExpectationSuiteIdentifier


def generate_expectation_suites_doc_site(
        abs_expectation_suites_path,
        abs_validation_path,
        abs_site_path
):
    data_context_config = DataContextConfig(
        plugins_directory=None,
        config_variables_file_path=None,
        datasources={
            "spark_ds": {
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                'execution_engine': {
                    'module_name': 'great_expectations.execution_engine',
                    'class_name': 'SparkDFExecutionEngine'
                },
                "data_connectors": {
                    "default_runtime_data_connector_name": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["batch_id"],
                    },
                },
            }
        },
        stores={
            "expectations_local_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": abs_expectation_suites_path,
                },
            },
            "validations_local_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": abs_validation_path,
                },
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"},
        },
        expectations_store_name="expectations_local_store",
        validations_store_name="validations_local_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        data_docs_sites={
            "s3_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": abs_site_path,
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": False,
                },
            }
        },
        anonymous_usage_statistics={
            "enabled": False
        }
    )

    context = BaseDataContext(project_config=data_context_config)

    resource_identifiers = []
    for folder in os.listdir(abs_expectation_suites_path):
        print('- {}'.format(folder))
        if not folder.startswith('.'):
            for suite in os.listdir(
                    os.path.join(abs_expectation_suites_path, folder)):
                print('  - {}'.format(suite))
                suite_identifier = ExpectationSuiteIdentifier(
                    expectation_suite_name="{}.{}".format(folder, suite[:-5]))
                resource_identifiers.append(suite_identifier)
    context.build_data_docs(resource_identifiers=resource_identifiers)

    return True


if __name__ == "__main__":
    generate_expectation_suites_doc_site(
        abs_expectation_suites_path='/app/src/expectation_suites',
        abs_validation_path='/app/src/validations',
        abs_site_path='/app/src/site'
    )
