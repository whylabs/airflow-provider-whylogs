import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="airflow-provider-whylogs",
    version="0.0.1",
    author="WhyLabs.ai",
    description="An Apache Airflow provider for whylogs",
    entry_points="""
        [apache_airflow_provider]
        provider_info=airflow_whylogs.__init__:get_provider_info
    """,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/whylabs/whylogs",
    install_requires=[
        "apache-airflow>=2.0",
        "whylogs>=1.0.10",
        "whylogs[viz]>=1.0.10"
    ],
    packages=[
        "whylogs_provider",
        "whylogs_provider.operators",
    ],
    python_requires=">=3.7",
    include_package_data=True,
)
