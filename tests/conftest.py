import pytest
import tempfile
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from delta_maintenance.models.models import (
    S3ClientDetails,
    S3KeyPairWrite,
    VirtualAddressingStyle,
    S3Scheme
)

@pytest.fixture(scope="session")
def s3key_pair_write() -> S3KeyPairWrite:
    return S3KeyPairWrite(
        hmac_keys_access_key_id="access-key-id",
        hmac_keys_secret_access_key="secret-access-key",
    )

@pytest.fixture(scope="session")
def s3_details(s3key_pair_write) -> S3ClientDetails:
    return S3ClientDetails(
        endpoint_host="localhost",
        region="eu-fr2",
        addressing_style=VirtualAddressingStyle.Path,
        port=5000,
        bucket="my-test-bucket",
        secrets=s3key_pair_write,
        scheme=S3Scheme.Http,
    )

@pytest.fixture(scope="session")
def tmp_path():
    return tempfile.TemporaryDirectory()

@pytest.fixture(scope="session")
def delta_table_path(tmp_path):
    # Create a temporary directory for the Delta table
    delta_table_path = (f"{tmp_path.name}/delta-table")
    
    # Initialize the Delta table with some mock data
    data = pd.DataFrame({
        "id": [1, 2],
        "name": ["Alice", "Bob"]
    })
    
    # Write the data to the Delta table
    write_deltalake(delta_table_path, data)
    
    yield delta_table_path