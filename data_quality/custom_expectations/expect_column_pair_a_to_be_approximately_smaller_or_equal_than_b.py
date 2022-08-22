import json

from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.expectations.expectation import \
    ColumnPairMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)

from great_expectations.expectations.util import \
    render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import substitute_none_for_missing
from great_expectations.render.types import RenderedStringTemplateContent, RenderedTableContent
from great_expectations.render.util import (
    num_to_str
)


class ColumnPairCustom(ColumnPairMapMetricProvider):
    condition_metric_name = "column_pair_values.a_approx_smaller_or_equal_than_b"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "ignore_row_if",
    )
    condition_value_keys = ("n_approximate",)

    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_A, column_B, **kwargs):

        assert (kwargs.get("n_approximate") is None) or \
               (type(kwargs.get("n_approximate")) == int), \
            "The paramenter n_approximate must be integer."

        if kwargs.get("n_approximate") is not None:
            approx = kwargs.get("n_approximate")
        else:
            approx = 0

        return column_A <= column_B + approx


class ExpectColumnPairAToBeApproximatelySmallerOrEqualThanB(ColumnPairMapExpectation):
    """
    Expect values in column A to be greater than column B.
    Args:
        column_A (str): The first column name
        column_B (str): The second column name
        n_approximate (int or None): additional approximation to column B value

    Keyword Args:
        allow_cross_type_comparisons (boolean or None): If True, allow
            comparisons between types (e.g. integer and string).
            Otherwise, attempting such comparisons will raise an exception.
        ignore_row_if (str): "both_values_are_missing",
            "either_value_is_missing", "neither"
        result_format (str or None): Which output mode to use:
            `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
        include_config (boolean): If True, then include the expectation config
            as part of the result object.
        catch_exceptions (boolean or None): If True, then catch exceptions and
            include them as part of the result object.
        meta (dict or None): A JSON-serializable dictionary (nesting allowed)
            that will be included in the output without modification.

    Returns:
        An ExpectationSuiteValidationResult
    """

    examples = [
        {
            "data": {
                "a": [11, 22, 50],
                "b": [10, 21, 100],
                "c": [9, 21, 30],
            },
            "schemas": {
                "spark": {
                    "a": "IntegerType",
                    "b": "IntegerType",
                    "c": "IntegerType"
                }
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "a", "column_B": "b", "n_approximate": 1},
                    "out": {"success": True},
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_A": "a", "column_B": "c"},
                    "out": {"success": False},
                },
            ],
        },
    ]

    map_metric = "column_pair_values.a_approx_smaller_or_equal_than_b"
    success_keys = (
        "column_A",
        "column_B",
        "ignore_row_if",
        "n_approximate",
        "mostly",
    )
    default_kwarg_values = {
        "mostly": 1.0,
        "ignore_row_if": "neither",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
            cls,
            configuration=None,
            result=None,
            language=None,
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
                "column_A",
                "column_B",
                "ignore_row_if",
                "n_approximate",
                "mostly",
                "row_condition",
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

        if params["n_approximate"] is not None:
            template_str = f"$column_A must always be smaller " \
                           f"or equal than $column_B plus $n_approximate" \
                           f"{mostly_str}."
        else:
            template_str = f"$column_A must be smaller " \
                           f"or equal than $column_B" \
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

        if result_dict.get("partial_unexpected_counts"):
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
                    table_rows.append([f"{result['expectation_config']['kwargs']['column_A']}: {value[0]}, "
                                       f"{result['expectation_config']['kwargs']['column_B']}: {value[1]}",
                                       count])
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
    self_check_report = ExpectColumnPairAToBeApproximatelySmallerOrEqualThanB().print_diagnostic_checklist()

    # test the custom expectation with the function `run_diagnostics()`
    # with great-expectations <= 0.14.7
    # self_check_report = ExpectColumnPairAToBeApproximatelySmallerOrEqualThanB().run_diagnostics()

    print(json.dumps(self_check_report, indent=2))
