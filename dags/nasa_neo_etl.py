from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from datetime import datetime, timedelta
from google.cloud import bigquery
import requests, json, pandas as pd
import os, sys
import logging

#Composer 3 setup
try:
    from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook
    HAS_SECRET_MANAGER = True
except ImportError:
    HAS_SECRET_MANAGER = False
    logging.warning("GoogleCloudSecretManagerHook not available - using Airflow Connections for local dev")

DAGS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, DAGS_FOLDER)

from create_neo_table import (
    create_partitioned_clustered_neo_table
)

NASA_SECRET_API_KEY = 'nasa-api-key'
PROJECT_ID = 'nasa-neo-pipeline'
DATAFORM_REGION = 'asia-southeast2'
DATAFORM_REPOSITORY_ID = 'nasa-neo-dataform'
GCS_RAW_DATA_PREFIX = 'raw_data/neo'
GCS_BUCKET = 'neo-nasa-data-nasa-neo-pipeline'

default_args = {
    'owner': 'jed.lanelle',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='dag_ingest_neo_nasa_api_bigquery',
    default_args=default_args,
    description='NASA NEO ETL Pipeline with incremental loading (Cloud Composer 3 + Local Dev)',
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags = ['composer3', 'gcs', 'secrets-manager', 'incremental', 'local-dev-compatible'],
)
def daily_neo_nasa_ingest_dag():

    @task
    def ensure_partition_table_exist():
        hook = BigQueryHook(
            gcp_conn_id='google_cloud_default', 
            location='asia-southeast2'
        )

        client = hook.get_client()

        try:
            table_id = f"{PROJECT_ID}.neo_data.neo_observations"
            table = client.get_table(table_id)

            has_partition = table.time_partitioning is not None
            has_clustering = table.clustering_fields is not None and len(table.clustering_fields) > 0
            
            if has_clustering and has_partition:
                logging.info("Table exists with partitioning and clustered")
                logging.info(f"Table partitioned by: {table.time_partitioning}")
                logging.info(f"Table clustered by: {table.clustering_fields}")
                return {"action": "exists", "optimized": True}
            else:
                if not has_partition:
                    logging.info("Table not partitioned")
                if not has_clustering:
                    logging.info("Table not clustered")
                client.delete_table(table_id)
                logging.info("Deleted non-optimized table")
                
                #Create partitioned version
                create_partitioned_clustered_neo_table(
                    project_id=PROJECT_ID,
                    dataset_id='neo_data',
                    table_id='neo_observations',
                    client=client
                )
                return {"action": "recreated", "optimized": True}
                
        except Exception as e:
            #Table doesn't exist - create it
            logging.info("Table doesn't exist, creating optimized table")
            create_partitioned_clustered_neo_table(
                project_id=PROJECT_ID,
                dataset_id='neo_data',
                table_id='neo_observations',
                client=client
            )
            return {"action": "created", "optimized": True}
        
    @task
    def get_nasa_api_key():
        #Composer or Local Docker
        is_composer = os.getenv('COMPOSER_ENVIRONMENT') is not None

        if is_composer and HAS_SECRET_MANAGER:
            try:
                hook = GoogleCloudSecretManagerHook(gcp_conn_id='google_cloud_default')
                response = hook.access_secret(
                    secret_id=NASA_SECRET_API_KEY,
                    project_id=PROJECT_ID,
                    secret_version='latest'
                )
                secret_value = response.payload.data.decode('utf-8')
                logging.info("Retrieved NASA API Key from Secret Manager (Cloud Composer)")
                return secret_value
            except Exception as e:
                logging.error(f"Failed to retrieve NASA API Key from Secret Manager: {e}")
                raise
        else:
            from airflow.hooks.base import BaseHook
            try:
                conn = BaseHook.get_connection('nasa_api')
                api_key = conn.extra_dejson.get('api_key')
                if not api_key:
                    raise ValueError("API key not found in nasa_api connection")
                logging.info("Retrieved NASA API Key from Airflow Connection (Local Dev)")
                return api_key
            except Exception as e:
                logging.error(f"Failed to retrieve NASA API Key from Airflow Connection: {e}")
                logging.info("Make sure 'nasa_api' connection is configured in Airflow UI")
                raise

    @task
    def validate_nasa_api_connection(api_key: str, **context):
        execution_date = context.get('ds')
        
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {
            'start_date': execution_date,
            'end_date': execution_date,
            'api_key': api_key
        }
        
        response = requests.get(url, params=params, timeout=30)
        logging.info(f"NASA API Status: {response.status_code}")
        logging.info(f"Found {len(response.json().get('near_earth_objects', {}))} dates with NEO data")
        if response.status_code != 200:
            raise ValueError(f"NASA API validation failed with status {response.status_code}")

        return True

    @task
    def validate_bigquery_connection():
        hook = BigQueryHook(
            gcp_conn_id='google_cloud_default', 
            location='asia-southeast2'
        )
        logging.info(f"Using Project ID: {PROJECT_ID}")
        
        query = f"""
        SELECT 
            1 as test_column,
            CURRENT_TIMESTAMP() as current_time
        """
        
        try:
            result = hook.get_pandas_df(sql=query, dialect='standard')
            logging.info(f"BigQuery Connection: SUCCESS")
            logging.info(f"Query Result: {result}")
            
            try:
                client = hook.get_client()
                dataset_ref = client.dataset('neo_data')
                tables = list(client.list_tables(dataset_ref))
                table_names = [table.table_id for table in tables]
                logging.info(f"Tables in neo_data dataset: {table_names if table_names else 'No tables yet'}")
            except Exception as e:
                logging.info(f"Dataset check: {str(e)}")
            
            return True
        except Exception as e:
            logging.info(f"BigQuery Connection: FAILED - {str(e)}")
            return False
    
    @task
    def extract_neo_data(api_key: str, **context):
        execution_date = context.get('ds')

        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {
            'start_date': execution_date,
            'end_date': execution_date,
            'api_key': api_key
        }

        logging.info(f"Fetching NEO data for date: {execution_date}")

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            
            gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
            gcs_path = f"{GCS_RAW_DATA_PREFIX}/neo_raw/{execution_date}.json"

            gcs_hook.upload(
                bucket_name=GCS_BUCKET,
                object_name=gcs_path,
                data=json.dumps(data, indent=2),
                mime_type='application/json'
            )
            
            logging.info(f'Successfully extracted NEO data for {execution_date}')
            logging.info(f'Uploaded to GCS - gs://{GCS_BUCKET}/{gcs_path}')
            logging.info(f"Total NEOs found: {data.get('element_count', 0)}")

            return gcs_path
        except requests.exceptions.RequestException as e:
            logging.error(f'Error fetching data from NASA API: {e}')
            return
    
    @task
    def load_to_bigquery(**context):
        gcs_path = context.get('task_instance').xcom_pull(task_ids='extract_neo_data')
        execution_date = context.get('ds')
        
        logging.info(f"Loading data from GCS Bucket - gs://{GCS_BUCKET}/{gcs_path}")
        
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        json_content = gcs_hook.download(
            bucket_name=GCS_BUCKET,
            object_name=gcs_path
        )

        data= json.loads(json_content.decode('utf-8'))

        records = []
        near_earth_objects = data.get('near_earth_objects', {})
        
        logging.info(f"Found {len(near_earth_objects)} date keys in near_earth_objects")
        logging.info(f"Total NEO count from API: {data.get('element_count', 0)}")
        
        for date_key, neo_list in near_earth_objects.items():
            logging.info(f"Processing {len(neo_list)} NEOs for date: {date_key}")
            
            for neo in neo_list:
                neo_id = neo.get('id')
                neo_name = neo.get('name', '').strip()
                neo_reference_id = neo.get('neo_reference_id')
                absolute_magnitude = neo.get('absolute_magnitude_h', 0)
                is_hazardous = neo.get('is_potentially_hazardous_asteroid', False)
                is_sentry = neo.get('is_sentry_object', False)
                nasa_jpl_url = neo.get('nasa_jpl_url', '')            
                diameter_data = neo.get('estimated_diameter', {}).get('meters', {})
                diameter_min = float(diameter_data.get('estimated_diameter_min', 0))
                diameter_max = float(diameter_data.get('estimated_diameter_max', 0))
                
                close_approaches = neo.get('close_approach_data', [])
                
                if not close_approaches:
                    logging.info(f"Warning: No close approach data for NEO {neo_id} ({neo_name})")
                    continue
                    
                for approach in close_approaches:
                    observation_date = pd.to_datetime(approach.get('close_approach_date')).date() if approach.get('close_approach_date') else None

                    velocity_kmh = approach.get('relative_velocity', {}).get('kilometers_per_hour', '0')
                    velocity_kmh = float(velocity_kmh.replace(',', '') if isinstance(velocity_kmh, str) else velocity_kmh)
                    
                    miss_distance = approach.get('miss_distance', {}).get('kilometers', '0')
                    miss_distance = float(miss_distance.replace(',', '') if isinstance(miss_distance, str) else miss_distance)
                    
                    miss_distance_astronomical = float(approach.get('miss_distance', {}).get('astronomical', 0))
                    miss_distance_lunar =  float(approach.get('miss_distance', {}).get('lunar', 0))

                    record = {
                        'neo_id': neo_id,
                        'neo_reference_id': neo_reference_id,
                        'name': neo_name,
                        'observation_date': observation_date,
                        'epoch_close_approach': approach.get('epoch_date_close_approach'),
                        'absolute_magnitude_h': absolute_magnitude,
                        'estimated_diameter_min_meters': diameter_min,
                        'estimated_diameter_max_meters': diameter_max,
                        'is_potentially_hazardous': is_hazardous,
                        'is_sentry_object': is_sentry,
                        'relative_velocity_kmh': velocity_kmh,
                        'miss_distance_km': miss_distance,
                        'miss_distance_astronomical': miss_distance_astronomical,
                        'miss_distance_lunar': miss_distance_lunar,
                        'orbiting_body': approach.get('orbiting_body', 'Earth'),
                        'nasa_jpl_url': nasa_jpl_url,
                        'ingestion_timestamp': pd.Timestamp.now(tz='UTC')
                    }
                    records.append(record)
        
        if not records:
            logging.warning("No NEO data to load for this date")
            return
        
        try:
            hook = BigQueryHook(
                gcp_conn_id='google_cloud_default',
                location='asia-southeast2'
            )
            
            dataset_id = 'neo_data'
            table_id = 'neo_observations'
            table_name = f"{PROJECT_ID}.{dataset_id}.{table_id}"
            
            df = pd.DataFrame(records)
            df['observation_date'] = pd.to_datetime(df['observation_date']).dt.date
            
            logging.info(f"Loading {len(df)} records to optimized BigQuery table: {table_name}")
            logging.info(f"Partition key: observation_date = {execution_date}")
            
            client = hook.get_client()

            delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{dataset_id}.{table_id}`
            WHERE observation_date = '{execution_date}'
            """

            try:
                client.query(delete_query).result()
                logging.info(f"Deleted existing data for {execution_date}")
            except Exception as e:
                logging.info(f"No existing data to delete (first run): {e}")
            
            job_config = bigquery.LoadJobConfig(
                schema=None,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            
            table_ref = client.dataset(dataset_id).table(table_id)
            job = client.load_table_from_dataframe(
                df, 
                table_ref, 
                job_config=job_config,
                location='asia-southeast2'
            )
            
            job.result()
            
            logging.info(f"✅ Successfully loaded {len(df)} records to partitioned table")
            logging.info(f"📅 Data loaded into partition: observation_date={execution_date}")
            
            verify_query = f"""
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT neo_id) as unique_neos,
                MIN(miss_distance_km) as closest_approach_km,
                SUM(CASE WHEN is_potentially_hazardous THEN 1 ELSE 0 END) as hazardous_count
            FROM {table_name}
            WHERE observation_date = '{execution_date}'
            """
            
            result = hook.get_pandas_df(sql=verify_query, dialect='standard')
            logging.info(f"\n📊 Load Verification for partition {execution_date}:")
            logging.info(f"- Records loaded: {result['record_count'][0]}")
            logging.info(f"- Unique NEOs: {result['unique_neos'][0]}")
            logging.info(f"- Hazardous NEOs: {result['hazardous_count'][0]}")
            logging.info(f"- Closest approach: {result['closest_approach_km'][0]:.2f} km")
            
            return {
                'records_loaded': int(result['record_count'][0]),
                'unique_neos': int(result['unique_neos'][0]),
                'execution_date': execution_date
            }
            
        except Exception as e:
            logging.error(f"Error loading data to BigQuery: {e}")
            raise

    create_compilation = DataformCreateCompilationResultOperator(
        task_id='dataform_compile',
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main"
        },
        trigger_rule='all_success',
    )

    run_assertions = DataformCreateWorkflowInvocationOperator(
        task_id='dataform_run_assertions',
        project_id=PROJECT_ID,
        region=DATAFORM_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('dataform_compile')['name'] }}"
        },
        trigger_rule='all_success',
    )

    #Set up dependency task
    table_check = ensure_partition_table_exist()
    get_api_key = get_nasa_api_key()
    nasa_connection = validate_nasa_api_connection(get_api_key)
    bigquery_connection = validate_bigquery_connection()
    fetch_neo_data = extract_neo_data(get_api_key)
    migration_task = load_to_bigquery()

    #Dependency chain
    table_check >> get_api_key >> nasa_connection >> bigquery_connection >> fetch_neo_data >> migration_task >> create_compilation >> run_assertions

dag = daily_neo_nasa_ingest_dag()