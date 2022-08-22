# Great Expectations core steps
In this folder you can find all you need to perform a first data quality 
run using Great Expectations (GE) library:
create a new Expectation Suite using native and Custom Expectations, validate 
the quality of your data and update the Data Docs. <br> 

Based on the [Great Expectations documentation](https://docs.greatexpectations.io/docs/), 
the data quality process is composed by 4 main steps:

![Ge steps](../images/ge_steps.png)

1. **Setup**: configure your environment. Define where to store
   the inputs (Expectations Suites), the outputs (Validations Results) of your 
   DQ checks and where to publish the Data Docs 
   (a human-readable renderings of your Expectation Suites 
   and Validation Results).
   
2. **Connect to data**: define your data source by setting the 
   Data Connector (configuration for connecting to different type of 
   external data source), and the Execution Engine (a system capable of 
   processing data to compute Expectations).
 
3. **Create Expectations**: create your Expectation Suite. Each Expectation is 
   a declarative, machine-verifiable assertion about the expected format, 
   content, or behavior of your data. In addition to built-in (native) 
   Expectations, it’s possible to develop your own custom Expectations too.
    
4. **Validate Data**: once you stored the Expectation Suite you can apply it
   to a fresh set of data through the validation step.


## Hands-on Data Quality steps
We provide a set of python files and notebooks to reproduce these steps and 
to give you the possibility to perform the data quality process over a sample 
dataset:

* **suite_dev_notebooks**: this folder contains `expectation_suite_template.ipynb`
notebook which shows you how to create a new Expectation Suite from an in-memory 
batch of data created over a Spark Data Frame.
Here you can test and check the performance of a new native expectation 
or Custom Expectation over the batch of data read from [sample_data.csv](../data).


* **custom_expectations**: here you can find 3 different types of
  Custom Expectations, a user defined expectation that, differently from the 
  [native ones](https://greatexpectations.io/expectations/), give you the 
  possibility to apply custom business checks to your data.
  Once a new Custom Expectation is developed you can call it in the 
  `suite_dev_notebooks/expectation_suite_template.ipynb` to run it over your
  data and check the output.
  The development of a Custom Expectation allows you to customize also its 
  Data Doc rendered description. 
  
* **generate_data_doc**: in the folder we provide a python script, executable 
  through the command `make ge-doc`, to create a new Data Docs based on your 
  Expectation Suites stored under the path `expectation_suites` 
  and to test your customized rendered descriptions.

* **validate_data**: once the Expectation Suite has been generated, you can run 
  the validation step over an in-memory Spark Data Frame following the template 
  `data_validation_with_checkpoints_template.ipynb` or running the python 
  script `data_validation_with_checkpoints.py` with the command 
  `make validate-data`.
  The output of the validation is a new directory named `validation` 
  containing the results of the just run validation and a new Data Docs 
  (stored under `site/validations` folder) updated with the latest results.