from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import requests
from airflow.models import Variable
import boto3
import json
import os

# default arguments
args = {
    'owner': 'airflow'
}

with DAG(
    dag_id='fetch_hubspot_engagements',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["hubspot"],
) as dag:

    s3_client = boto3.client('s3')

    # secrets may live in environment or in variables
    bucket = Variable.get("metadata_bucket", None) if Variable.get(
        "metadata_bucket", None) else os.getenv("metadata_bucket")

    def get_http_response(url) -> dict:
        """
        sends GET request to the provided endpoint url
        """
        token = Variable.get("hs_token", None) if Variable.get(
            "hs_token", None) else os.getenv("hs_token")
        payload = {}
        headers = {
            'Authorization': f"Bearer {token}"}

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
            return response.json()
        else:
            return response.text

    def get_properties_meta():
        """
        Get All Available custom objects
        """
        schema_url = "https://api.hubapi.com/properties/v1/contacts/properties"
        properties_meta = get_http_response(schema_url)
        return properties_meta

    def compile_properties(meta:list):
        out_list = []
        for obj in meta:
            name = obj.get("name")
            out_list.append(name)
        return out_list



    def put_to_s3(s3_client, bucket, data, key):
        """
        uses the boto3 client to put object in S3 storage
        """
        s3_client.put_object(Body=json.dumps(data),
                             Bucket=bucket,
                             Key=key)
        return f"Object Put Success: {key}"


    def gen_object_url(properties,engagement_type,limit=100,after=None):
        """
        """
        base_url = f"https://api.hubapi.com/crm/v3/objects/{engagement_type}?"
        properties_str = "".join(map(lambda x: "&properties="+x, properties))
        limit = f"limit={limit}"
        after = '' if not after else f"&after={after}"
        associations = "&associations=contacts"
        return base_url+limit+after+properties_str+associations



    def read_object_data(properties, **context):
        after = None
        counter = 0
        iterate = True

        url = gen_object_url(properties, limit=100, after=after)
        print(f"Fetch URL ->  {url}")
        data = get_http_response(url)
        counter += 1
        # spark has trouble reading colons
        task_id = context["run_id"].replace(":","")

        while iterate:
            url = gen_object_url(properties, limit=100, after=after)
            # fetch the key results
            data = get_http_response(url)
            results = data.get("results")
            counter += 1
            # put object to s3
            put_to_s3(s3_client=s3_client, bucket=bucket, data=results,
                      key=f"contacts/{task_id}_{counter}.json")
            after = data.get("paging").get("next").get("after") if data.get("paging") else None
            
            if not after:
                iterate = False
            print(f"next entity id {after}")

        return "All data download complete"

    # fetch all properties
    properties = Variable.get("contact_properties").split("\n")


    collect_meta_task = PythonOperator(
            task_id=f"collect_contact_props_all",
            python_callable=read_object_data,
            op_kwargs={"properties": properties}
        )


    collect_meta_task