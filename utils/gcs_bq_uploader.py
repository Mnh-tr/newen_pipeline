import json
import pandas as pd
import glob

from google.cloud import storage
from io import BytesIO
from google.cloud import bigquery
from google.cloud import storage
from google.auth import exceptions
from loguru import logger

CRE_PATH = "configs/service_account.json"

def sql_from_bigquery(query, CRE_PATH):
    """Fetch data from BigQuery."""
    bq_client = bigquery.Client.from_service_account_json(CRE_PATH)
    
    job = bq_client.query(query)
    results = job.result()    
    df = results.to_dataframe()

    return df

def upload_gcs(bucket_name,upload_path,upload_file,original_path_file):
        
    client = storage.Client.from_service_account_json(CRE_PATH)
    bucket = client.get_bucket(bucket_name)            
    blob = bucket.blob(upload_path+upload_file)    
    with open(original_path_file, 'rb') as source_file:
        blob.upload_from_file(source_file)
    
    logger.debug(original_path_file + " : gcs upload done")

# def load_gcs(bucket_name,prefix):
#     lists = gcs_list(bucket_name,prefix)    
#     for blob in lists:
#         df = pd.DataFrame(gcs_read(bucket_name,blob))
#         print(df)
        
#     return df

def load_df_gcs(bucket_name, prefix):
    lists = gcs_list(bucket_name, prefix)
    for blob in lists:
        data = gcs_read(bucket_name, blob)
        df = pd.DataFrame(data)
        print(df)
    return df

def load_gcs(bucket_name, prefix):
    lists = gcs_list(bucket_name, prefix)
    for blob in lists:
        data = gcs_read(bucket_name, blob)
    return data

# def load_gcs_json(bucket_name,prefix):
#     json_data_list = []
#     lists = gcs_list(bucket_name,prefix)    
#     for blob in lists:
#         content = gcs_read(bucket_name, blob)
#         json_data_list.append(json.loads(content))
        
#     return json_data_list

def upload_to_bigquery(df,project_id,dataset_id,table_id):
    bq_client = bigquery.Client.from_service_account_json(CRE_PATH)
    
    try:
        table_ref = bq_client.dataset(dataset_id).table(table_id)
    except:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True  # Automatically detect schema
    
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        logger.debug('BQ upload is done: ' + project_id + '/' + dataset_id + '/' + table_id)
    except exceptions.GoogleAuthError as e:
        logger.error('Failed to upload to BigQuery. Error:', e)
    except Exception as e:
        logger.error('An error occurred:', e)
        pass

def upload_to_bigquery_uten_shop(df,project_id,dataset_id,table_id):
    try:
        bq_client = bigquery.Client.from_service_account_json(CRE_PATH)
    except exceptions.GoogleAuthError as e:
        logger.error('Failed to authenticate with Google Cloud. Error:', e)
        return

    try:
        table_ref = bq_client.dataset(dataset_id).table(table_id)
    except:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig()

    job_config.autodetect = True  

    try:
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        logger.debug('BQ upload is done: {}/{}'.format(dataset_id, table_id))
    except Exception as e:
        logger.error('An error occurred:', e)
        
def upload_to_bigquery_for_new(df, project_id, dataset_id, table_id):
    bq_client = bigquery.Client.from_service_account_json(CRE_PATH)

    try:
        table_ref = bq_client.dataset(dataset_id).table(table_id)
    except:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

    df['images'] = df['images'].apply(lambda x: json.dumps(json.loads(x)) if x is not None else "null")

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True  # 스키마 자동 감지

    try:
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print('BQ 업로드가 완료되었습니다: ' + project_id + '/' + dataset_id + '/' + table_id)
    except exceptions.GoogleAuthError as e:
        print('BigQuery로 업로드 실패. 오류:', e)
    except Exception as e:
        print('오류가 발생했습니다:', e)
        pass

def gcs_list(bucket_name,prefix_):
    list = []

    storage_client = storage.Client.from_service_account_json(CRE_PATH)
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix_)
    
    for blob in blobs:
        list.append(blob.name)       
    
    return list

# def gcs_read(bucket_name,blob_name):
    
#     storage_client = storage.Client.from_service_account_json(CRE_PATH)
#     bucket = storage_client.get_bucket(bucket_name)
#     blob = bucket.get_blob(blob_name)       
        
#     data = blob.download_as_text()    
#     data = json.loads(data)   
#     return data
def gcs_read(bucket_name, blob_name):
    storage_client = storage.Client.from_service_account_json(CRE_PATH)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    
    # Check if blob exists
    if blob is None:
        raise FileNotFoundError(f"Blob '{blob_name}' not found in bucket '{bucket_name}'")
    
    # Download and parse the JSON data
    data = blob.download_as_text()
    try:
        data = json.loads(data)  # Ensure valid JSON format
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to decode JSON data from blob '{blob_name}': {e}")
    
    return data





def delete_bucket(bucket_name):
    """Deletes a bucket. The bucket must be empty."""
    storage_client = storage.Client.from_service_account_json(CRE_PATH)

    bucket = storage_client.bucket(bucket_name)

    try:
        bucket.delete()
        print(f'Bucket {bucket_name} deleted.')
    except Exception as e:
        print(f'Error deleting bucket {bucket_name}: {e}')

def delete_all_objects(bucket_name):
    """Deletes all objects in the specified GCS bucket."""
    storage_client = storage.Client.from_service_account_json(CRE_PATH)
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs()
    for blob in blobs:
        blob.delete()
        print(f'Blob {blob.name} deleted.')


def gcs_list_(bucket_name, prefix_):
    list = []

    storage_client = storage.Client.from_service_account_json(CRE_PATH)
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix_)

    for blob in blobs:
        list.append(blob.name)

    return list

def load_gcs_(bucket_name, prefix):
    lists = gcs_list_(bucket_name, prefix)

    df = pd.DataFrame()

    for blob in lists:
        blob_data = pd.DataFrame(gcs_read(bucket_name, blob))
        df = pd.concat([df, blob_data])

    return df

def load_gcs_json(bucket_name, blob_name):
    json_data_list = []
    content = gcs_read(bucket_name, blob_name)  # Already returns a dictionary
    if isinstance(content, dict):
        json_data_list.append(content)  # Append the dictionary directly
    else:
        json_data_list.append(json.loads(content))  # Handle cases where content is not parsed
    return json_data_list

def load_all_json_files_from_gcs(bucket_name, file_prefix):
    try:
        client = storage.Client.from_service_account_json(CRE_PATH)
        bucket = client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=file_prefix)

        all_json_data = []

        for blob in blobs:
            try:
                content = blob.download_as_text()
                json_data = json.loads(content)
                all_json_data.extend(json_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from {blob.name}: {e}")
            except Exception as e:
                print(f"Error processing {blob.name}: {e}")
        df = pd.DataFrame(all_json_data)
        return df

    except Exception as e:
        print(f"Error connecting to the GCS bucket: {e}")
        return None


def load_all_json_files_from_gcs2(bucket_name, file_prefix):
    try:
        client = storage.Client.from_service_account_json(CRE_PATH)
        
        bucket = client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=file_prefix)

        all_json_data = []

        for blob in blobs:
            try:
                content = blob.download_as_text()

                json_data = json.loads(content)
                all_json_data.extend(json_data)

            except json.JSONDecodeError as e:
                print(f"Error decoding JSON from {blob.name}: {e}")
            except Exception as e:
                print(f"Error processing {blob.name}: {e}")

        return all_json_data

    except Exception as e:
        print(f"Error connecting to the GCS bucket: {e}")
        return None
