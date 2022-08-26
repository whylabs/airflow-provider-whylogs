import whylogs_provider
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="airflow-provider-whylogs",
    version=whylogs_provider.__version__,
    author="WhyLabs.ai",
    description="An Apache Airflow provider for whylogs",
    entry_points="""
        [apache_airflow_provider]
        provider_info=whylogs_provider.__init__:get_provider_info
    """,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/whylabs/airflow-provider-whylogs",
    install_requires=[
        "apache-airflow>=2.0",
        "whylogs[viz, s3]>=1.0.10"
    ],
    packages=[
        "whylogs_provider",
        "whylogs_provider.operators",
    ],
    python_requires=">=3.7",
    include_package_data=True,
)
