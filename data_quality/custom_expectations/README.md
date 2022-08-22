# Custom Expectations

In this package we store all the Custom Expectations which can be imported during
the Expectation Suite development to perform custom test over data.

Each Custom Expectation python module contains two classes:
In the first class you have to describe the metric to validate the data 
and the engine used (Spark in this case).
In the second class you define the expectation name, the output format, the 
parameters default values, the success keys metrics and the aspect
of the expectation in the data docs (named the rendering part). <br> 

> **_NOTE:_** The second class name must be the name of the expectation in 
_CamelCaseName_ like format. During the Expectation Suite development the 
custom expectation can be added to a validator object by calling the name 
specified at this stage in the _snake_case_name_ format (check the 
[expectation_suite_template.ipynb](../suite_dev_notebooks/expectation_suite_template.ipynb)
file for an example).

In this repository we show three different type of Custom Expectation developed
for `SparkDFExecutionEngine` data source:
1. Single Column Custom Expectations;
2. Pair Columns Custom Expectations;
3. Multi Columns Custom Expectations.

## Single Column Custom Expectations
This type of Custom Expectations allow you to perform custom checks over a single 
column of the table. In the template file `expect_column_length_match_input_length.py`, 
we show how to develop a Pyspark Single Column Custom Expectation which 
allows you to check when the length of values of an input column are equal to
a given input number.

The python module is composed by two classes:

- In `ColumnMetricCustom` class we define the `condition_metric_name` and 
`condition_value_keys` variables which represent respectively the id of the 
custom metric, and a tuple of custom input parameters (the _"length"_ in this case). 
Once defined these two parameters, we develop the custom control inside the
 `_spark()` method which must be a PySpark like syntax control. <br>
- In `ExpectColumnLengthMatchInputLength` class we create the `example` variable, 
which contains a set of tests that you can call via `pytest` to validate the 
metric previously defined, and the customized rendering part by setting the 
variable `template_str` inside the `_prescriptive_renderer()` module. 
The last feature gives the possibility to have a custom description of our 
Custom Expectation on the data docs site.

## Pair Columns Custom Expectations
The Pair Columns Custom Expectations allow you to perform checks between two
columns of a batch of data. Here we developed a template file 
`expect_column_pair_a_to_be_approximately_smaller_or_equal_than_b.py` which
helps you to check when a given column is less or equal than another plus a 
given input number.

- In `ColumnPairCustom` class, differently from the Single Columns, we have to 
add the variable `condition_domain_keys` which shows the inputs of the Pyspark 
check: columns `column_A` and `column_B`.
- In `ExpectColumnPairAToBeApproximatelySmallerOrEqualThanB` class, 
as in the Single Column, we developed the tests (inside the variable `example`), 
and the rendering part using the two extra variables `column_A` and `column_B` 
in the custom string `template_str`. In addition to that, we add a new 
method named `_diagnostic_unexpected_table_renderer()` that, accordingly to GE doc,
allows to add a diagnostic table to the validation results of a custom 
expectation. 

> **_NOTE:_** Based on the GE doc, the Pair Column Custom Expectations perform 
this rendered block natively, but not in the way we expected. 
Thus, we decided to customize the module template in order to have a
rendered table which contains also the name of the column used in the custom 
check.

## Multi Columns Custom Expectations
The Multi Columns Custom Expectations allow you to perform checks between 
multiple columns of a batch of data. In the file
`expect_multicolumn_customer_id_user_id_device_id.py` we developed a template
that helps you to perform a custom check over three different fields.

- In `MulticolumnCustomMetric` class we have the same variable as in the 
pair column case but, instead of `column_A` and `column_B` parameters, the
multiple columns are passed as a list of columns inside the `column_list` 
parameter which is used by the `_spark()` method to perform the custom check.
- In `ExpectMulticolumnCustomerIdUserIdDeviceId` we developed the tests (inside 
the variable `example`), and in the `_prescriptive_renderer()` method we 
customized the expectation data docs.<br/>
Last step, as in the Pair Column case, we added the
`_diagnostic_unexpected_table_renderer()` module to customize the validation
doc site of our Custom Expectation.
- 
> **_NOTE:_** Due to the 
[#4295](https://github.com/great-expectations/great_expectations/issues/4295) 
issue, the template file slightly differ from the one used in the Pair Column, 
precisely at the `if result_dict.get(partial_unexpected_counts):` statement step. 
This issue prevents us to use the validation parameter 
`partial_unexpected_count` to construct the diagnostic table of a Multicolumn
Custom Expectation. <br>
As a workaround we construct our diagnostic table based on the
`partial_unexpected_list` parameter, a list of `column_list` configurations
which did not pass the expectation logic.

## _Bonus_: Testing Custom Expectations with PyCharm Professional

You can locally run custom expectation tests with the checks implemented in 
the `example` variable.
To use the [previously built Docker image](../../README.md#expectation-suite-development-with-jupyter-notebook) 
as a Remote Python Interpreter:

1. Open PyCharm Professional and import the Project.
2. Under File, choose `Settings...` (for Mac, under PyCharm, choose Preferences)
3. Under Settings, choose Project Interpreter. Click the gear icon, choose 
`Show All..` from the drop-down menu.
4. Choose the `+` icon and create a new Docker interpreter selecting the image 
`mediaset-data-quality-jupyter-dev` and press `OK`.
5. Edit the `Run/Debug Configurations` of the project to properly launch the 
docker image
6. Insert the `Script path` selecting the path of the module `.py` that you 
   want to run (in this case the custom expectation module you want to test)
7. Press `Run` button.
8. The Run console will show you the output of the `.py` module.

