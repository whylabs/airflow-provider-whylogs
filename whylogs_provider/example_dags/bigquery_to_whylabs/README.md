# Writing BigQuery tables to WhyLabs with Airflow
This is an example repository on how to profile a Google BigQuery table and write it to 
WhyLabs for monitoring. It uses a local deployment of Airflow for demonstration purposes, 
so caution is necessary when reproducing this for production use-cases. 

### Setup environment
First off, make sure you have Python>=3.7 and setup a virtual environment to install the `requirements.txt` file.
```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```
After it finishes, you can spin up the Airflow instance by running `airflow standalone`.

### (Optional) Define the DAGs folder
You can either copy this DAG file to your DAGs folder or change the `airflow.cfg` to point the `dags_folder` to this directory. 

### Service Account with BigQuery privileges
1. Look for the service account to which you want to grant BigQuery editor permissions. If the service account does not yet exist, you will need to create one by going to "IAM & Admin" > "Service accounts" and clicking "Create Service Account".
2. Click the pencil icon next to the service account to edit its permissions. Click "Add Another Role" if it already has assigned roles. In the "Select a role" dropdown, select the "BigQuery Editor" role from the list.
3. Click "Save" to apply the changes. 

This grants the service account permissions to perform tasks associated with the role in BigQuery.

### Setup the GCP connection in Airflow under google_cloud_default
On the Airflow UI, create a new connection by going to Admin > Connections and use the following:
```
Connection ID: google_cloud_default
Connection Type: Google Cloud
Project ID: <your-project-id>
Keyfile JSON: <JSON content from the service-account.json>
``` 

The JSON file can be downloaded from the Google Cloud Console on: IAM > Service Accounts > Keys.
Another option is to use the `Keyfile path` argument, which points to a local path where the file is located on your machine. `Keyfile JSON` stores the Service Account access to the Airflow's database.

## Setup WhyLabs credentials in Airflow
On the Airflow UI, go to Admin > Variables > "+" and create the following variables:
```
WHYLABS_API_KEY = <your-api-key>
WHYLABS_DEFAULT_ORG_ID = <your-org-id>
WHYLABS_DEFAULT_DATASET_ID = <your-dataset-id>
```
These will be accessed by the DAG to authenticate with WhyLabs and write your profiles.

## Run the DAG!
After doing all these steps, you can unpause your DAG and trigger it to make sure everything is working correctly. Modify the `query` and `project_id` variables on `bigquery_whylabs_example_dag.py` to meet your project's criteria. 

## Good-to-know 
We have had proxying issues, where no external endpoint could be reached from within the Airflow tasks when running this example on a MacOS machine. To overcome this, set the environment 
```bash
export NO_PROXY="*"
```
