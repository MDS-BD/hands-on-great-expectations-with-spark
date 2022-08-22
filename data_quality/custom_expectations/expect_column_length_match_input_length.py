import json

from great_expectations.execution_engine import (
   SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
   ColumnMapMetricProvider,
   column_condition_partial,
)

from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    substitute_none_for_missing,
    num_to_str,
)

import pyspark.sql.functions as f


class ColumnMetricCustom(ColumnMapMetricProvider):

    condition_metric_name = "column_values.match_input_length"
    condition_value_keys = ("length",)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, length=None, **kwargs):
        assert length, "length parameter could not be None."
        assert type(length) == int, "length must be of type integer."

        return (f.length(column) == length) & \
               (column.isNotNull())


class ExpectColumnLengthMatchInputLength(ColumnMapExpectation):
    """
    Expect that the column is not null and that it satisfies the
    given length of characters.

    Args:
        column (str): The column name to evaluate
        length (int). Length to satisfy

    Keyword Args:
        result_format (str or None):  Which output mode to use:
        `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
        Default set to `BASIC`.

    Returns:
        An ExpectationSuiteValidationResult
    """

    examples = [
        {
            "data": {
                "a": ["V00001111", "V12345678"],
                "b": ["V01", "V123456789"],
            },
            "schemas": {
                "spark": {
                    "a": "StringType",
                    "b": "StringType"
                }
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "length": 9},
                    "out": {"success": True},
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "b", "length": 9},
                    "out": {"success": False},
                },
            ],
        },
    ]

    map_metric = "column_values.match_input_length"

    # A tuple consisting of values that must / could be provided by the user
    # and defines how the Expectation evaluates success.
    success_keys = ("length", "mostly",)

    # (Optional) - Default values for success keys and the defined domain,
    # among other values.
    default_kwarg_values = {
        "regex": None,
        "result_format": "BASIC",
        "mostly": 1
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
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "length",
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

        template_str = f"values length must match the input length of $length" \
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


if __name__ == "__main__":
    # test the custom expectation with the function
    # `print_diagnostic_checklist()` with great-expectations >= 0.14.8
    self_check_report = ExpectColumnLengthMatchInputLength().print_diagnostic_checklist()

    # test the custom expectation with the function `run_diagnostics()`
    # with great-expectations <= 0.14.7
    # self_check_report = ExpectColumnLengthMatchInputLength().run_diagnostics()

    print(json.dumps(self_check_report, indent=2))
