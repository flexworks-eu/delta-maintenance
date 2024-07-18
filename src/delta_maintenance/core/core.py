from deltalake import DeltaTable

from delta_maintenance.models.models import S3ClientDetails
from delta_maintenance.result import Result, Ok, Err
from typing import Any

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
            dt = DeltaTable(
                    self.delta_table_path,
                    storage_options=self.client_details.to_s3_config().unwrap() 
                )
        except Exception as e:
            raise Exception(f"Error initializing DeltaTable: {str(e)}")
        
        return dt
        

    def compact_table(self) -> Result[dict[str, Any], str]:
        try:
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
            result = self.delta_table.create_checkpoint()
            return Ok(result)
        except Exception as e:
            return Err(f"Error creating checkpoint for DeltaTable: {str(e)}")

        
    
def process_delta_table(delta_table_path: str) -> None:
    delta_table = DeltaTable(delta_table_path)

