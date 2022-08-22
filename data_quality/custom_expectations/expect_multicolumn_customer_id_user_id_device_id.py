import json

from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)

from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import substitute_none_for_missing
from great_expectations.render.types import RenderedStringTemplateContent, RenderedTableContent
from great_expectations.render.util import (
    num_to_str
)


class MulticolumnCustomMetric(MulticolumnMapMetricProvider):

    condition_metric_name = "multicolumn_values.customer_id"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "ignore_row_if",
    )

    condition_value_keys = ("device_id_regex",)

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        device_id_regex = kwargs.get("device_id_regex")
        assert device_id_regex is not None, "device_id_regex parameter must be set."
        assert type(device_id_regex) == str, "device_id_regex must be of type string."

        return (column_list[0].isNotNull()) \
               & ((column_list[1].isNotNull() &
                   (column_list[0] == column_list[1]))
                | (
                 (column_list[1].isNull()) &
                (column_list[2].rlike(device_id_regex)) &
                (column_list[0] == column_list[2]))
               )


class ExpectMulticolumnCustomerIdUserIdDeviceId(MulticolumnMapExpectation):
    """
    Expect that the column `customer_id` is equal to `user_uid` when this is
    not empty; otherwise `customer_id` must be equal to bd:`bd_device_id`.

     Args:
         column_list (tuple or list): The column names to evaluate

     Keyword Args:
         ignore_row_if (str): default to "never"
         result_format (str or None):  Which output mode to use:
            `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            Default set to `BASIC`.
         include_config (boolean): If True, then include the expectation
            config as part of the result object.
            Default set to True.
         catch_exceptions (boolean or None): If True, then catch exceptions
            and include them as part of the result object.
            Default set to False.

     Returns:
         An ExpectationSuiteValidationResult
    """

    examples = [
        {
            "data": {
                "a": ["d001", "1000", "1001"],
                "b": [None, "1000", "1001"],
                "c": ["d001", "d002", "d002"],
                "d": ["d001", "d002", "1001"],
            },
            "schemas": {
                "spark": {
                    "a": "StringType",
                    "b": "StringType",
                    "c": "StringType",
                    "d": "StringType"
                }
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["a", "b", "c"],
                           "device_id_regex": "d[0-9]{3}$"},
                    "out": {"success": True},
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["d", "b", "c"],
                           "device_id_regex": "d[0-9]{3}$"},
                    "out": {"success": False},
                },
            ],
        },
    ]

    map_metric = "multicolumn_values.customer_id"
    success_keys = ("device_id_regex", "mostly",)
    default_kwarg_values = {
        "ignore_row_if": "never",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "mostly": 1,
    }

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
            cls,
            configuration=None,
            runtime_configuration=None,
            **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name",
                                                        True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")

        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column_list",
                "device_id_regex",
                "ignore_row_if",
                "mostly",
            ],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=5, no_scientific=True
            )
        mostly_str = (
            ""
            if params.get("mostly") is None
            else ", at least $mostly_pct % of the time"
        )

        params["column_list_customer_id"] = params["column_list"][0]
        params["column_list_user_id"] = params["column_list"][1]
        params["column_list_device_id"] = params["column_list"][2]

        template_str = f"$column_list_customer_id must be equal to " \
                       f"$column_list_user_id when this is not empty, " \
                       f"or equal to $column_list_device_id and match the " \
                       f"regex $device_id_regex when $column_list_user_id is empty" \
                       f"{mostly_str}."

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.unexpected_table")
    def _diagnostic_unexpected_table_renderer(
            cls,
            configuration=None,
            result=None,
            language=None,
            runtime_configuration=None,
            **kwargs,
    ):
        try:
            result_dict = result.result
        except KeyError:
            return None

        if result_dict is None:
            return None

        if not result_dict.get(
                "partial_unexpected_list") and not result_dict.get(
            "partial_unexpected_counts"
        ):
            return None

        table_rows = []

        # Based on the https://github.com/great-expectations/great_expectations/issues/4295
        # we add a condition to ignore the partial_unexpected_counts parameter in case of
        # hashable type error for MulticolumnMapExpectation and render only the
        # partial_unexpected_list.

        if result_dict.get("partial_unexpected_counts") and not \
            result_dict["partial_unexpected_counts"][0]["error"] == \
                "partial_exception_counts requires a hashable type":
            # We will check to see whether we have *all* of the unexpected values
            # accounted for in our count, and include counts if we do. If we do not,
            # we will use this as simply a better (non-repeating) source of
            # "sampled" unexpected values
            total_count = 0

            for unexpected_count_dict in result_dict.get(
                    "partial_unexpected_counts"):
                if not isinstance(unexpected_count_dict, dict):
                    # handles case: "partial_exception_counts requires a hashable type"
                    # this case is also now deprecated (because the error is moved to an errors key
                    # the error also *should have* been updated to "partial_unexpected_counts ..." long ago.
                    # NOTE: JPC 20200724 - Consequently, this codepath should be removed by approximately Q1 2021
                    continue
                value = unexpected_count_dict.get("value")
                count = unexpected_count_dict.get("count")
                total_count += count
                if value is not None and value != "":
                    table_rows.append([value, count])
                elif value == "":
                    table_rows.append(["EMPTY", count])
                else:
                    table_rows.append(["null", count])

            # Check to see if we have *all* of the unexpected values accounted for. If so,
            # we show counts. If not, we only show "sampled" unexpected values.
            if total_count == result_dict.get("unexpected_count"):
                header_row = ["Unexpected Value", "Count"]
            else:
                header_row = ["Sampled Unexpected Values"]
                table_rows = [[row[0]] for row in table_rows]
        else:
            header_row = ["Sampled Unexpected Values"]
            sampled_values_set = set()
            for unexpected_value in result_dict.get("partial_unexpected_list"):
                if unexpected_value:
                    string_unexpected_value = str(unexpected_value)
                elif unexpected_value == "":
                    string_unexpected_value = "EMPTY"
                else:
                    string_unexpected_value = "null"
                if string_unexpected_value not in sampled_values_set:
                    table_rows.append([unexpected_value])
                    sampled_values_set.add(string_unexpected_value)

        unexpected_table_content_block = RenderedTableContent(
            **{
                "content_block_type": "table",
                "table": table_rows,
                "header_row": header_row,
                "styling": {
                    "body": {"classes": ["table-bordered", "table-sm", "mt-3"]}
                },
            }
        )

        return unexpected_table_content_block


if __name__ == "__main__":
    # test the custom expectation with the function
    # `print_diagnostic_checklist()` with great-expectations >= 0.14.8
    self_check_report = ExpectMulticolumnCustomerIdUserIdDeviceId().print_diagnostic_checklist()

    # test the custom expectation with the function `run_diagnostics()`
    # with great-expectations <= 0.14.7
    # self_check_report = ExpectMulticolumnCustomerIdUserIdDeviceId().run_diagnostics()

    print(json.dumps(self_check_report, indent=2))
