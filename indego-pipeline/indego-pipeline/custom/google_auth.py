if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# default credentials will default to the attached service account
import google.auth
credentials, project = google.auth.default()



# credentials = compute_engine.Credentials()

from google.cloud import storage
from google.cloud import bigquery
from google.cloud import dataproc_v1

@custom
def transform_custom(*args, **kwargs):

    # insert functions to test connectivity to cloud services
    # summarize in df for test function

    return 1


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'