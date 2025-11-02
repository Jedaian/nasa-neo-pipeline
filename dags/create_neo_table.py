from google.cloud import bigquery
import json, os, sys
import logging

DAGS_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, DAGS_FOLDER)

def create_partitioned_clustered_neo_table(
    project_id: str,
    dataset_id: str,
    table_id: str,
    client = None
):
    if client is None:
        client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Load schema from JSON file
    schema_path = os.path.join(DAGS_FOLDER, 'config', 'neo_schema.json')
    with open(schema_path, 'r') as f:
        schema_fields = json.load(f)
    
    # Convert schema to BigQuery SchemaField objects
    schema = []
    for field in schema_fields:
        schema.append(
            bigquery.SchemaField(
                name=field['name'],
                field_type=field['type'],
                mode=field['mode'],
                description=field.get('description', '')
            )
        )
    
    # Create table object with schema
    table = bigquery.Table(table_ref, schema=schema)
    
    # Configure PARTITIONING
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,  # Daily partitions
        field="observation_date",                  # Partition column
        # Optional: Add expiration for old partitions (e.g., delete after 3 years)
        # expiration_ms=1000 * 60 * 60 * 24 * 365 * 3  # 3 years in milliseconds
    )
    
    # Optional: Require partition filter in queries (prevents accidental full scans)
    table.require_partition_filter = True

    table.clustering_fields = [
        "is_potentially_hazardous",
        "is_sentry_object",
        "orbiting_body"
    ]

    logging.info(f"Table clustered by: {table.clustering_fields}")
    
    # Add table description
    table.description = """
    NASA Near Earth Objects (NEO) observation data
    - Partitioned by observation_date for optimal query performance
    - Contains asteroid approach data, physical characteristics, and hazard assessments
    """
    
    # Create the table
    try:
        table = client.create_table(table)
        logging.info(f"✅ Successfully created partitioned table: {table_ref}")
        logging.info(f"📅 Partitioned by: observation_date (DAILY)")
        logging.info(f"🔒 Partition filter required: {table.require_partition_filter}")
        logging.info(f"\nTable details:")
        logging.info(f"  - Project: {project_id}")
        logging.info(f"  - Dataset: {dataset_id}")
        logging.info(f"  - Table: {table_id}")
        logging.info(f"  - Total fields: {len(schema)}")
        
    except Exception as e:
        if "Already Exists" in str(e):
            logging.info(f"⚠️  Table {table_ref} already exists")
            logging.info(f"💡 To recreate with partitioning, you need to:")
            logging.info(f"   1. Delete the existing table")
            logging.info(f"   2. Run this script again")
        else:
            logging.info(f"❌ Error creating table: {e}")
            raise