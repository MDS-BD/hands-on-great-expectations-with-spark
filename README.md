# Hands-on Great Expectations with Spark

_This is the companion repository related to the article "[How to monitor Data Lake health status at scale](https://medium.com/towards-data-science/how-to-monitor-data-lake-health-status-at-scale-d0eb058c85aa)" published on **Towards Data Science** tech blog._
<br/>
_Checkout the project presentation at the [Great Expectations Community Meetup](https://youtu.be/WLXffNScH8U)._ 

In this repository you can find a complete guide to perform Data Quality checks 
over an in-memory Spark dataframe using the  python package 
[Great Expectations](https://greatexpectations.io/).

In detail, cloning and browsing the repository will show you how to:
1. Create a new Expectation Suite over an in-memory Spark dataframe;
2. Add Custom Expectations to your Expectation Suite;
3. Edit the Custom Expectations output description and the validation Data Docs;
4. Generate and update the Data Docs website;
5. Execute a Validation run over an in-memory Spark dataframe.

## How to navigate the project

Folder structure:

- [data quality](data_quality/README.md): here is stored the core of the project. 
  <br/>
  In this folder you can find:
  - a template to start to develop a new Expectation Suite using Native 
  Expectations and Custom Expectations
  - Custom Expectation templates, one for each expectation type: `single_column`, 
  `pair_column` and `multicolumn`
  - code to generate (and update) the Great Expectations Data Docs
  - code to run Data Validation

- [expectation_suites](expectation_suites/README.md): here is stored the 
  Expectation Suite generated from the execution of the jupyter notebook.<br/>
  The directory, auto-generate by Great Expectation, follows the naming 
  convention `expectation_suites/dataset_name/suite_name.json`. 

- [data](data/README.md): here is stored the data used for this hands-on. <br/>
  The dataset `sample_data.csv` was used either to develop the Expectation Suite 
  and to run the Data Validation.

## How and where to start

In the **Makefile** are listed a set of commands which will help you to browse and 
use this project.

### Expectation suite development with Jupyter Notebook

Expectation Suites dev env is based on a Jupyter Notebooks instance running in 
a Docker container. The Docker Image used to run the container, is also 
adopted as Remote Python Interpreter with PyCharm Professional to develop 
Custom Expectations with the support of an IDE.

The development environment run on a Docker container with:
 - Jupyter Notebook
 - Python 3.8
 - Spark 3.1.1

**Before to start**: install [Docker](https://docs.docker.com/).

1. Build the Docker image which contains all that you need to start to develop 
  Great Expectation Suites running the command:

   ```makefile
   make build
   ```

2. Run Docker container from the previously built image with the command:
   
   ```makefile
   make run
    ```
3. To reach Jupyter Notebook click on the url that you can find on the terminal.

### How to run data validation

To validate the data (available in the folder `data`) run the command:

```makefile
make validate-data
```
This will generate the folders `validations/` and `site/` which contain 
respectively results of the data quality validation run and the auto-generated 
data documentation.

### How to generate Great Expectations Data Docs

To locally generate only the Great Expectations data documentation run the 
command:

```makefile
make ge-doc
```
This will generate `site/` folder with the data documentation auto-generated by 
Great Expectations.

## Contributors

- [Davide Romano](https://www.linkedin.com/in/davideromano90/) - _Mediaset Business Digital_
- [Nicola Saraceni](https://www.linkedin.com/in/nicola-saraceni-9228b0127/) - _Mediaset Business Digital_
