import pytest

from delta_maintenance.models.models import (
    S3ClientDetails,
    S3KeyPairWrite,
    VirtualAddressingStyle,
    S3Scheme,
)
from delta_maintenance.result import Result, Ok, Err



def test_s3_details_default() -> None:
    default_result = S3ClientDetails.default()
    # default_result = s3_details.default()
    assert default_result.is_ok()
    assert default_result.unwrap() == S3ClientDetails(
        endpoint_host="s3.amazonaws.com",
        region="us-east-1",
        addressing_style=VirtualAddressingStyle.Virtual,
        port=443,
        bucket=None,
        hmac_keys=None,
        scheme=S3Scheme.Https,
    )

def test_s3_config(s3_details: S3ClientDetails) -> None:
    config_result = s3_details.to_s3_config()
    assert config_result.is_ok()
    config = config_result.unwrap()

    assert config["aws_virtual_hosted_style_request"] == s3_details.addressing_style.value
    assert config["endpoint"] == f"{s3_details.scheme.value}://{s3_details.endpoint_host}:{s3_details.port}"