import argparse
import datetime
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

# import custom_expectations package
import sys
sys.path.append('../')
import custom_expectations


def get_logger(logger_name, logger_level):
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, logger_level.upper()))

    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, logger_level.upper()))
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def main():
    # global CONF
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_name',
                        help='Name of the dataset to validate',
                        required=True)
    parser.add_argument('--suite_name',
                        help='The expectation suite name',
                        required=True)
    parser.add_argument('--log_level',
                        help='The log level',
                        required=True)

    args, unknown_args = parser.parse_known_args()

    logger = get_logger(logger_name=__file__,
                        logger_level=args.log_level)

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info('Spark session created')

    logger.info("Name of the dataset to validate: {}, "
                .format(args.dataset_name))
    expectation_suite = args.dataset_name+"."+args.suite_name
    logger.info("Expectation suite name: {}, "
                .format(expectation_suite))

    logger.info('Set up spark schema')
    schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("time_spent", IntegerType(), True),
        StructField("video_duration", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("device_id", StringType(), True)
    ])

    logger.info('Reading dataset...')
    df = spark.read.format("csv") \
        .option("sep", ",") \
        .option("nullValue", "*") \
        .option("header", "true") \
        .option("escape", "\"") \
        .schema(schema) \
        .load("/home/jovyan/work/data/{}.csv".format(args.dataset_name))
    logger.info('Dataset successfully read')

    datasources = {
        "filesystem_datasource": {
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "SparkDFExecutionEngine",
                "force_reuse_spark_context": "true"
            },
            "data_connectors": {
                "runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"],
                },
            },
        }
    }

    logger.info('Instantiating Great Expectations Data Context...')
    data_context_config = DataContextConfig(
        datasources=datasources,
        stores={
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "/home/jovyan/work/expectation_suites",
                }
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "/home/jovyan/work/validations",
                }
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"},
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "/home/jovyan/work/checkpoints",
                }
            }
        },
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        checkpoint_store_name="checkpoint_store",
        data_docs_sites={
            "dq_website": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "/home/jovyan/work/site",
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
    logger.info('Great Expectations Data Context instantiated ')

    logger.info('Reading RuntimeBatchRequest...')
    batch_request = RuntimeBatchRequest(
        datasource_name="filesystem_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="data_asset_name",
        batch_identifiers={"batch_id": "something_something"},
        runtime_parameters={"batch_data": df},
    )

    validations = [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite
         }
    ]

    checkpoint = SimpleCheckpoint(
        name="checkpoint",
        data_context=context,
        class_name="SimpleCheckpoint",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction"
                }
            }
        ]
    )

    logger.info('Validation Checkpoint running...')

    run_id = {
        "run_name": args.dataset_name + "_" + args.suite_name + "_run",
        "run_time": datetime.datetime.now(datetime.timezone.utc)
    }

    checkpoint_result = checkpoint.run(
        run_id=run_id,
        run_name_template="%Y%m%d_%H%M%S",
        validations=validations,
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction"
                }
            }
        ]
    )

    logger.info('Validation Checkpoint completed')


if __name__ == '__main__':
    main()
