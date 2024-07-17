 
from pydantic import BaseModel
from enum import Enum
from typing import Optional

from delta_maintenance.result import Result, Ok, Err


class AddressingStyle(Enum):
    Virtual: str = "virtual"
    Path: str = "path"


class S3KeyPairWrite(BaseModel):
    hmac_keys_access_key_id: str
    hmac_keys_secret_access_key: str
 
class S3ClientDetails(BaseModel):
    endpoint_host: str
    region: str
    addressing_style: AddressingStyle
    port: int
    bucket: Optional[str]
    secrets: Optional[S3KeyPairWrite]

    def default(self) -> Result['S3ClientDetails', str]:
        return Ok(S3ClientDetails(
            endpoint_host="s3.amazonaws.com",
            region="us-east-1",
            addressing_style=AddressingStyle.Virtual,
            port=443,
            bucket=None,
            secrets=None,
            )
        )

    def endpoint_url(self) -> Result[str, str]:
        """
        Constructs the endpoint URL based on the addressing style.

        This function generates the endpoint URL for the s3 client based on whether
        the addressing style is virtual or path-based.

        Returns:
            Result[str, str]: A Result object containing the constructed URL as a string
            if successful, or an error message as a string if unsuccessful.
        """
        match self.addressing_style:
            case AddressingStyle.Virtual:
                return Ok(f"https://{self.bucket}.{self.endpoint_host}:{self.port}"),
            case AddressingStyle.Path:
                return Ok(f"https://{self.endpoint_host}:{self.port}")

    def to_s3_config(self) -> dict[str, str]:
        """
        Converts the S3 client details into a dictionary suitable for S3 configuration.

        This function generates a dictionary containing the necessary configuration
        parameters for connecting to an S3-compatible service using the s3 client details.

        Returns:
            dict[str, str]: A dictionary with the following keys:
                - "aws_virtual_hosted_style_request": The addressing style value (either "virtual" or "path").
                - "AWS_REGION": The region of the s3 service.
                - "AWS_ACCESS_KEY_ID": The access key ID for s3 HMAC keys.
                - "AWS_SECRET_ACCESS_KEY": The secret access key for s3 HMAC keys.
                - "endpoint": The constructed endpoint URL based on the addressing style.
                - "AWS_S3_ALLOW_UNSAFE_RENAMES": A flag to allow unsafe renames in S3 (set to "true").
        """
        return Ok({
            "aws_virtual_hosted_style_request": self.addressing_style.value,
            "AWS_REGION": self.region,
            "AWS_ACCESS_KEY_ID": self.secrets.hmac_keys_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.secrets.hmac_keys_secret_access_key,
            "endpoint": self.endpoint_url(),
            "AWS_S3_ALLOW_UNSAFE_RENAMES": "true",
        })
