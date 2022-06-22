import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="airflow-provider-whylogs",
    version="0.0.1",
    author="WhyLabs",
    description="An Apache Airflow provider for whylogs",
    entry_points="""
        [apache_airflow_provider]
        provider_info=great_expectations_provider.__init__:get_provider_info
    """,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/great-expectations/airflow-provider-great-expectations",
    install_requires=[
        "apache-airflow>=2.1",
        "whylogs>=1.0.0",
        "pandas",
        "fsspec",
        "s3fs"
    ],
    packages=[
        "whylogs_provider",
        "whylogs_provider.operators",
    ],
    python_requires=">=3.7",
    include_package_data=True,
)
