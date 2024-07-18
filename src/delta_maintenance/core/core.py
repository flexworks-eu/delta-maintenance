import logging
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from delta_maintenance.models.models import S3ClientDetails
from delta_maintenance.result import Result, Ok, Err
from typing import Any
from delta_maintenance.models.models import (
    S3ClientDetails,
    S3Scheme,
    VirtualAddressingStyle,
    S3KeyPairWrite
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def hello_world(arg: str) -> str:
    return f"hello {arg}"

class DeltaTableProcessor:
    def __init__(self,
        delta_table_path: str,
        client_details: S3ClientDetails,
    ):
        self.delta_table_path = delta_table_path
        self.client_details = client_details
        self.delta_table = self._init_table()

    def _init_table(self) -> DeltaTable:
        dt: DeltaTable
        try:
            storage_options: S3ClientDetails
            if self.client_details is not None:
                storage_options = self.client_details.to_s3_config().unwrap()
                dt = DeltaTable(
                        self.delta_table_path,
                        storage_options=storage_options, 
                    )
            else:
                dt = DeltaTable(
                        self.delta_table_path,
                    )
        except TableNotFoundError as e:
            logger.error(f"DeltaTable {self.delta_table_path} not found: {str(e)}")
            return 
        except Exception as e:
            raise Exception(f"Error initializing DeltaTable: {str(e)}")
        
        return dt
        

    def compact_table(self) -> Result[dict[str, Any], str]:

        try:
            logger.error(f"table version: {self.delta_table.version()}")
            result = self.delta_table.optimize.compact()
            return Ok(result)
        except Exception as e:
            return Err(f"Error compacting DeltaTable: {str(e)}")

    def vacuum_table(self,
            retention_hours: int = 168,
            enforce_retention_duration: bool = False,
            dry_run: bool = True
        ) -> Result[dict[str, Any], str]:
        try:
            result = self.delta_table.vacuum(
                retention_hours=retention_hours,
                enforce_retention_duration=enforce_retention_duration,
                dry_run=dry_run)
            return Ok(result)
        except Exception as e:
            return Err(f"Error vacuuming DeltaTable: {str(e)}")
        
    def create_checkpoint(self) -> Result[dict[str, Any], str]:
        try:
            _ = self.delta_table.create_checkpoint()
            return Ok("Checkpoint created successfully.")
        except Exception as e:
            return Err(f"Error creating checkpoint for DeltaTable: {str(e)}")
    
    def table_version(self) -> Result[int, str]:
        try:
            return Ok(self.delta_table.version())
        except Exception as e:
            return Err(f"Error getting table version: {str(e)}")


# def set_client_details(
#     bucket_name: None,
#     endpoint_host: str = None,
#     access_key: str = None,
#     secret_key: str = None,
#     addressing_style: str = "virtual",
#     region: str = "us-east-1",
#     port: int = 443,
#     allow_unsafe_https: bool = False,
#     scheme: str = "https",
# ) -> Result[S3ClientDetails, str]:



#     client_details = S3ClientDetails.default()




def delta_compact(delta_table_path: str,
                  client_details: S3ClientDetails = None
        ) ->  Result[dict[str, Any], str]:

    processor = DeltaTableProcessor(delta_table_path, client_details)
    if processor.delta_table is None:
        return Err(f"DeltaTable {delta_table_path} not found.")
    return processor.compact_table()


def delta_vacuum(delta_table_path: str,
            client_details: S3ClientDetails = None,
            retention_hours: int = 168,
            enforce_retention_duration: bool = False,
            dry_run: bool = True
    ) -> Result[dict[str, Any], str]:
    processor = DeltaTableProcessor(delta_table_path, client_details)
    if processor.delta_table is None:
        return Err(f"DeltaTable {delta_table_path} not found.")    
    return processor.vacuum_table(
        retention_hours=retention_hours,
        enforce_retention_duration=enforce_retention_duration,
        dry_run=dry_run,
    )


def delta_create_checkpoint(delta_table_path: str,
                            client_details: S3ClientDetails = None
        ) -> Result[dict[str, Any], str]:
    processor = DeltaTableProcessor(delta_table_path, client_details)
    if processor.delta_table is None:
        return Err(f"DeltaTable {delta_table_path} not found.")
    return processor.create_checkpoint()

def delta_version(delta_table_path: str,
                  client_details: S3ClientDetails = None
    ) -> Result[int, str]:
    processor = DeltaTableProcessor(delta_table_path, client_details)
    if processor.delta_table is None:
        return Err(f"DeltaTable {delta_table_path} not found.")
    return processor.table_version()