import pytest

from delta_maintenance.models.models import (
    S3ClientDetails,
    S3KeyPairWrite,
    AddressingStyle,
)
from delta_maintenance.result import Result, Ok, Err


@pytest.fixture
def s3key_pair_write() -> S3KeyPairWrite:
    return S3KeyPairWrite(
        hmac_keys_access_key_id="access-key-id",
        hmac_keys_secret_access_key="secret-access-key",
    )

@pytest.fixture
def s3_details(s3key_pair_write) -> S3ClientDetails:
    return S3ClientDetails(
        endpoint_host="s3.amazonaws.com",
        region="us-east-1",
        addressing_style=AddressingStyle.Virtual,
        port=443,
        bucket="my-bucket",
        secrets=s3key_pair_write,
    )

def test_s3_details_default(s3_details: S3ClientDetails) -> None:
    default_result = s3_details.default()
    assert default_result.is_ok()
    assert default_result.unwrap() == S3ClientDetails(
        endpoint_host="s3.amazonaws.com",
        region="us-east-1",
        addressing_style=s3_details.addressing_style,
        port=443,
        bucket=None,
        secrets=None,
    )

def test_s3_config(s3_details: S3ClientDetails) -> None:
    config_result = s3_details.to_s3_config()
    assert config_result.is_ok()
    config = config_result.unwrap()

    assert config["aws_virtual_hosted_style_request"] == s3_details.addressing_style.value