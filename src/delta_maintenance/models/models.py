 
from pydantic import BaseModel
from enum import Enum
from typing import Optional

from delta_maintenance.result import Result, Ok, Err

class S3Scheme(Enum):
    Http: str = "http"
    Https: str = "https"

class VirtualAddressingStyle(Enum):
    Virtual: str = "true"
    Path: str = "false"

class S3KeyPairWrite(BaseModel):
    access_key_id: Optional[str]
    secret_access_key: Optional[str]
 
class S3ClientDetails(BaseModel):
    endpoint_host: str = None
    region: str = None
    port: int = None
    addressing_style: Optional[VirtualAddressingStyle] = VirtualAddressingStyle.Virtual
    bucket: Optional[str] = None
    hmac_keys: Optional[S3KeyPairWrite] = None
    scheme: Optional[S3Scheme] = None
    allow_unsafe_https: bool = False

    @staticmethod
    def default() -> Result['S3ClientDetails', str]:
        return Ok(S3ClientDetails(
            endpoint_host="s3.amazonaws.com",
            region="us-east-1",
            port=443,
            addressing_style=VirtualAddressingStyle.Virtual,
            bucket=None,
            hmac_keys=None,
            scheme=S3Scheme.Https,
            allow_unsafe_https=False,
            )
        )

    def endpoint_url(self) -> Result[str, str]:
        match self.addressing_style:
            case VirtualAddressingStyle.Virtual:
                return Ok(f"{self.scheme.value}://{self.bucket}.{self.endpoint_host}:{self.port}")
            case VirtualAddressingStyle.Path:
                return Ok(f"{self.scheme.value}://{self.endpoint_host}:{self.port}")

    def to_s3_config(self) -> Result[dict[str, str], str]:


        return Ok({
            "aws_virtual_hosted_style_request": self.addressing_style.value,
            "AWS_REGION": self.region,
            "AWS_ACCESS_KEY_ID": self.hmac_keys.access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.hmac_keys.secret_access_key,
            "endpoint": self.endpoint_url().unwrap(),
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true" if self.allow_unsafe_https else "false",
        })

class ClientDetails(BaseModel):
    delta_path: str
    s3client_details: Optional[S3ClientDetails] = None
