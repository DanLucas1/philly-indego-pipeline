if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# google client libraries
import google.auth
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import dataproc_v1
import pandas as pd

# default credentials will resolve to the attached service account
credentials, project = google.auth.default()

@custom
def transform_custom(*args, **kwargs):

    resources = {
        'resource':[],
        'type':[]
        }

    # test service account access to storage buckets
    storage_client = storage.Client()
    buckets = storage_client.list_buckets()
    
    for bucket in buckets:
        resources['type'].append('cloud storage bucket')
        resources['resource'].append(bucket.name)


    # test service account access to bigquery datasets

    bq_client = bigquery.Client()
    datasets = bq_client.list_datasets()

    for dataset in datasets:
        resources['type'].append('bigquery dataset')
        resources['resource'].append(dataset.dataset_id)


    # test service account access to dataproc

    project_id = "indego-pipeline"
    region = "us-central1"

    # ClusterControllerClient with regional endpoint
    client_options = {'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    cluster_client = dataproc_v1.ClusterControllerClient(client_options=client_options)
    
    # list clusters
    clusters = cluster_client.list_clusters(project_id=project_id, region=region)

    for cluster in clusters:
        resources['type'].append('dataproc cluster')
        resources['resource'].append(cluster.cluster_name)


    # summarize cloud resources in df for test function
    resources_df = pd.DataFrame.from_dict(resources)
    return resources_df


@test
def test_output(output, *args) -> None:
    print(type(output))
    # assert 'indego_815299289556' in output['type'].value_counts(), 'missing storage bucket'
    # assert 'indego_tripdata' in output['type'].value_counts(), 'missing bigquery dataset '
    # assert 'indego-cluster' in output['type'].value_counts(), 'missing dataproc cluster'
    # assert output is not None, 'The output is undefined'
test_output(transform_custom())