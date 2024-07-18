from click.testing import CliRunner
from delta_maintenance.cli.cli import vacuum, compact, create_checkpoint
from deltalake import DeltaTable, write_deltalake
import pandas as pd
import pytest
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_cli_delta_table_with_data(cli_delta_table_path):
    # delta_table_path = f"{delta_table_path}_cli"
    dt = DeltaTable(cli_delta_table_path)
    logger.error(f"{cli_delta_table_path=}")

    assert dt.version() == 0

    for i in range(10):
        data = pd.DataFrame({
            "id": [i+2, i+3],
            "name": [f"{str(i)}_name", f"{str(i)}_other_name"]
        })

        write_deltalake(cli_delta_table_path, data, mode="append")

    dt_result = dt = DeltaTable(cli_delta_table_path)

    assert dt_result.version() == 10
    assert len(dt_result.to_pandas()) == 22

    assert len(dt_result.files()) == 11

    # shutil.copytree(cli_delta_table_path, "/tmp/delta_table_backup")

    # yield cli_delta_table_path


def test_compact(cli_delta_table_path):
  runner = CliRunner()
  result = runner.invoke(compact, [cli_delta_table_path])
  assert result.exit_code == 0
  time.sleep(2)
#   assert "numFilesAdded" in result.output

# def test_vacuum( cli_delta_table_path):
#   runner = CliRunner()
#   result = runner.invoke(vacuum, [cli_delta_table_path,
#                                   "--retention-hours",
#                                   0,
#                                   "--force",
#                                   "--disable-retention-duration"]
#                         )
#   assert result.exit_code == 0
#   assert "parquet" in result.output
#   time.sleep(0.5)


# def test_create_checkpoint( cli_delta_table_path):
#     runner = CliRunner()
#     result = runner.invoke(create_checkpoint, [cli_delta_table_path])

#     assert result.exit_code == 0
#     assert "Checkpoint created successfully" in result.output
#     time.sleep(0.5)