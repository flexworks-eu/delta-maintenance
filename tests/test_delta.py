# moto, moto_server, and other mocks are not stable enough for me to depend on

from deltalake import DeltaTable, write_deltalake
import pandas as pd
import logging
import time
import os
import pytest

from delta_maintenance.core.core import DeltaTableProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




@pytest.fixture(scope="session")
def delta_table_with_data(delta_table_path):
    dt = DeltaTable(delta_table_path)
    logger.error(f"{delta_table_path=}")

    assert dt.version() == 0

    for i in range(10):
        data = pd.DataFrame({
            "id": [i+2, i+3],
            "name": [f"{str(i)}_name", f"{str(i)}_other_name"]
        })

        write_deltalake(delta_table_path, data, mode="append")

    dt_result = dt = DeltaTable(delta_table_path)

    assert dt_result.version() == 10
    assert len(dt_result.to_pandas()) == 22

    assert len(dt_result.files()) == 11

    yield dt_result


def test_init_delta_table(delta_table_with_data, delta_table_path, s3_details):
    delta_processor = DeltaTableProcessor(delta_table_path, s3_details)
    logger.error(f"{delta_table_path=}")
    # dt_init = delta_processor._init_table()

    # assert dt_init.is_ok()

    dt = delta_processor.delta_table

    assert dt.version() == 10
    assert len(dt.files()) == 11


@pytest.fixture(scope="session")
def initialized_delta_table(delta_table_with_data, delta_table_path, s3_details):
    delta_processor = DeltaTableProcessor(delta_table_path, s3_details)
    logger.error(f"{delta_table_path=}")
    # # dt_init = delta_processor._init_table()

    # assert dt_init.is_ok()

    # dt = dt_init.unwrap()
    return delta_processor

def test_compact_table(initialized_delta_table, delta_table_path):
    processor = initialized_delta_table

    result = processor.compact_table()

    assert result.is_ok()

    logger.error(f"{result.unwrap()=}")

    # dt_result.optimize.compact()

    compacted_dt_result = DeltaTable(delta_table_path)
    assert compacted_dt_result.version() == 11
    assert len(compacted_dt_result.to_pandas()) == 22
    assert len(compacted_dt_result.files()) == 1


def test_vacuum_table(initialized_delta_table, delta_table_path):
    processor = initialized_delta_table

    # this should fail because retention_hours is 0 < 168
    result = processor.vacuum_table(retention_hours=0, enforce_retention_duration=True, dry_run=False)
    assert result.is_err()
    assert "Invalid retention period" in result.unwrap_err()

    result = processor.vacuum_table(retention_hours=0, enforce_retention_duration=False, dry_run=False)
    assert result.is_ok()
    logger.error(f"{result.unwrap()=}")

    vacuumed_dt_result = DeltaTable(delta_table_path)
    assert vacuumed_dt_result.version() == 13
    assert len(vacuumed_dt_result.to_pandas()) == 22
    assert len(vacuumed_dt_result.files()) == 1
    files = os.listdir(delta_table_path)

    assert len(files) == 2
    assert any([f.startswith("part") for f in files])
    assert any([f.startswith("_delta_log") for f in files])

    # # assert (compacted_dt_result.history()) == 4
    compacted_dt_result = DeltaTable(delta_table_path)
    history = compacted_dt_result.history()
    assert len(history) == 14
    assert history[0]["operation"] == "VACUUM END"
    assert history[1]["operation"] == "VACUUM START"
    assert history[2]["operation"] == "OPTIMIZE"
    for i in range(3,13):
        assert history[i]["operation"] == "WRITE"
    assert history[13]["operation"] == "CREATE TABLE"


def test_create_checkpoint(initialized_delta_table, delta_table_path):
    processor = initialized_delta_table
    checkpoint_files = os.listdir(f"{delta_table_path}/_delta_log")

    assert '_last_checkpoint' not in checkpoint_files

    result = processor.create_checkpoint()
    
    assert result.is_ok()
    logger.error(f"{result.unwrap()=}")

    last_result = DeltaTable(delta_table_path)
    # non-operation
    assert last_result.version() == 13
    assert last_result.history()[0]["operation"] == "VACUUM END"

    checkpoint_files = os.listdir(f"{delta_table_path}/_delta_log")

    assert '_last_checkpoint' in checkpoint_files





    


