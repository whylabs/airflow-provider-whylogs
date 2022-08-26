# whylogs Airflow Operator

This is a package for the [whylogs](https://github.com/whylabs/whylogs) provider, the open source standard for data and ML logging. With whylogs, users are able to generate summaries of their datasets (called whylogs profiles) which they can use to:

- Track changes in their dataset
- Create data constraints to know whether their data looks the way it should
- Quickly visualize key summary statistics about their datasets

This Airflow operator focuses on simplifying whylogs' usage along with Airflow. Users are encouraged to benefit from their existing Data Profiles, which are created with whylogs and can bring a lot of value and visibility to track their data changes over time.  
 
## Installation

You can install this package on top of an existing Airflow 2.0+ installation ([Requirements](#requirements)) by simply running:

```bash
$ pip install airflow-provider-whylogs
```

To install this provider from source, run these instead:

```bash
$ git clone git@github.com:whylabs/airflow-provider-whylogs.git
$ cd airflow-provider-whylogs
$ python3 -m venv .env && source .env/bin/activate
$ pip3 install -e .
```

## Usage example

In order to benefir from the existing operators, users will have to profile their data first, with their **processing** environment of choice. To create and store a profile locally, run the following command on a pandas DataFrame:

```python
import whylogs as why

df = pd.read_csv("some_file.csv")
results = why.log(df)
results.writer("local").write()
```

And after that, you can use our operators to either:

- Create a Summary Drift Report, to visually help you identify if there was drift in your data

```python
from whylogs_provider.operators.whylogs import WhylogsSummaryDriftOperator

summary_drift = WhylogsSummaryDriftOperator(
        task_id="drift_report",
        target_profile_path="data/profile.bin",
        reference_profile_path="data/profile.bin",
        reader="local",
        write_report_path="data/Profile.html",
    )
```

- Run a Constraints check, to check if your profiled data met some criteria

```python
from whylogs_provider.operators.whylogs import WhylogsConstraintsOperator
from whylogs.core.constraints.factories import greater_than_number

constraints = WhylogsConstraintsOperator(
        task_id="constraints_check",
        profile_path="data/profile.bin",
        reader="local",
        constraint=greater_than_number(column_name="my_column", number=0.0),
    )
```

>**NOTE**: It is important to note that even though it is possible to create a Dataset Profile with the Python Operator, Airflow tries to separate the concern of orchestration from processing, so that is one of the reasons why we didn't want to have a strong opinion on how to read data and profile it, enabling users to best adjust this step to their existing scenario.

A full DAG example can be found on the whylogs_provider package [directory](https://github.com/whylabs/airflow-provider-whylogs/tree/mainline/whylogs_provider/example_dags).  

## Requirements

The current requirements to use this Airflow Provider are described on the table below. 

| PIP package        | Version required |
|--------------------|------------------|
| ``apache-airflow`` | ``>=2.0``      |
| ``whylogs[viz, s3]``   | ``>=1.0.10``     |

## Contributing

Users are always welcome to ask questions and contribute to this repository, by submitting issues and communicating with us through our [community Slack](http://join.slack.whylabs.ai/). Feel free to reach out and make `whylogs` even more awesome to use with Airflow.

Happy coding! 😄
