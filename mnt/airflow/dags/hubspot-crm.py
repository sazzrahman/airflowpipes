## Hubspot CRM Objects Download

from airflow import DAG
from airflow.decorators import dag,task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import requests
from airflow.models import Variable
import boto3
import json
import os

# default arguments
args = {
    'owner':'airflow'
}

with DAG(
    dag_id='fetch_hubspot_crm_objects',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["hubspot"],
) as dag:

    s3_client = boto3.client('s3')

    # secrets may live in environment or in variables
    bucket = Variable.get("metadata_bucket",None) if Variable.get("metadata_bucket",None) else os.getenv("metadata_bucket")

    def get_http_response(url)->dict:
        """
        sends GET request to the provided endpoint url
        """
        token = Variable.get("hs_token",None) if Variable.get("hs_token",None) else os.getenv("hs_token")
        payload={}
        headers = {
        'Authorization': f"Bearer {token}"}

        response = requests.request("GET", url, headers=headers, data=payload)

        if response.status_code == 200:
            return response.json()
        else:
            return response.text


    def get_schema():
        """
        Get All Available custom objects
        """
        schema_url = "https://api.hubapi.com/crm/v3/schemas"
        custom_objects = get_http_response(schema_url)
        return custom_objects["results"]

    def put_to_s3(s3_client,bucket,data,key):
        """
        uses the boto3 client to put object in S3 storage
        """
        s3_client.put_object(Body=json.dumps(data),
        Bucket=bucket,
        Key=key)
        return f"Object Put Success: {key}"

    def compile_objects(custom_objects):
        """
        """
        out_list = []
        for obj in custom_objects:
            label,type_id = get_object_id(obj)
            properties = get_properties(obj)
            out_list.append({"objectTypeId":type_id,"label":label,"properties":properties})
        return out_list

    
    def collect_metadata(ti):
        """
        collects schema metadata of all custom objects
        """
        schema = get_schema()
        obj_list = compile_objects(schema)
        ti.xcom_push("custom_obj_list",obj_list)



    def get_object_id(custom_object:dict)->list:
        """
        object id and labels are return from a nested custom object
        """
        label = custom_object.get("labels").get("singular")
        type_id = custom_object.get("objectTypeId")
        return label,type_id


    def get_properties(custom_object:dict)->list:
        """
        Input: A dict object of custom objects in Hubspot's shcema
        Output: A list of all properties
        """
        properties = custom_object.get("properties")
        out_list = []
        for item in properties:
            out_list.append(item.get("name"))
        return out_list

    def gen_object_url(meta,limit=100,after=''):
        """
        generate object url based on all custom properties
        """
        objectTypeId = meta["objectTypeId"]
        properties = meta["properties"]  
        base_url = f"https://api.hubapi.com/crm/v3/objects/{objectTypeId}?"
        properties_str = "".join(map(lambda x: "&properties="+x, properties))
        limit = f"limit={limit}"
        return base_url+limit+after+properties_str

    
    def create_task_branch(ti):
        for item in ti.xcom_pull("custom_obj_list"):
            idx = item.get("objectTypeId")
            task=PythonOperator(
            task_id = f"{idx}",
            python_callable=read_object_data
            )

        return task
            
    
    def read_object_data():
        print("hello world")
        


    

    collect_meta_task=PythonOperator(
        task_id="collect_metadata",
        python_callable=collect_metadata
    )



    


    collect_meta_task>>create_task_branch()




