 
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
    hmac_keys_access_key_id: str
    hmac_keys_secret_access_key: str
 
class S3ClientDetails(BaseModel):
    endpoint_host: str
    region: str
    port: int
    addressing_style: Optional[VirtualAddressingStyle] = VirtualAddressingStyle.Virtual
    bucket: Optional[str] = None
    secrets: Optional[S3KeyPairWrite] = None
    scheme: Optional[S3Scheme] = None
    allow_unsafe_https: bool = False

    def default(self) -> Result['S3ClientDetails', str]:
        return Ok(S3ClientDetails(
            endpoint_host="s3.amazonaws.com",
            region="us-east-1",
            port=443,
            addressing_style=VirtualAddressingStyle.Virtual,
            bucket=None,
            secrets=None,
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
            "AWS_ACCESS_KEY_ID": self.secrets.hmac_keys_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.secrets.hmac_keys_secret_access_key,
            "endpoint": self.endpoint_url().unwrap(),
            "AWS_S3_ALLOW_UNSAFE_RENAMES": "true",
            "AWS_ALLOW_HTTP": "true" if self.allow_unsafe_https else "false",
        })
