__version__ = "0.0.2"

def get_provider_info():
    return {
        "package-name": "airflow-provider-whylogs",
        "name": "whylogs Provider",
        "description": "An Apache Airflow provider for Data and ML Monitoring with whylogs.",
        "versions": [__version__],
    }