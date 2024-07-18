from deltalake import DeltaTable, write_deltalake
import pandas as pd
import logging
import os
import pytest
import boto3

import signal
import requests
import time
import shutil
import subprocess as sp

from delta_maintenance.core.core import (
    delta_compact,
    delta_vacuum,
    delta_create_checkpoint,
    delta_version
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def start_service(service_name, host, port):
    moto_svr_path = shutil.which("moto_server")
    args = [moto_svr_path, "-H", host, "-p", str(port)]
    process = sp.Popen(
        args, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE
    )  # shell=True
    url = "http://{host}:{port}".format(host=host, port=port)
 
    for i in range(0, 30):
        output = process.poll()
        if output is not None:
            print("moto_server exited status {0}".format(output))
            stdout, stderr = process.communicate()
            print("moto_server stdout: {0}".format(stdout))
            print("moto_server stderr: {0}".format(stderr))
            pytest.fail("Can not start service: {}".format(service_name))
 
        try:
            # we need to bypass the proxies due to monkeypatches
            requests.get(url, timeout=5)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(0.5)
    else:
        stop_process(process)  # pytest.fail doesn't call stop_process
        pytest.fail("Can not start service: {}".format(service_name))
 
    return process
 

def stop_process(process):
    try:
        process.send_signal(signal.SIGTERM)
        process.communicate(timeout=20)
    except sp.TimeoutExpired:
        process.kill()
        time.sleep(3)
        outs, errors = process.communicate(timeout=20)
        exit_code = process.returncode
        msg = "Child process finished {} not in clean way: {} {}".format(
            exit_code, outs, errors
        )
        raise RuntimeError(msg)
 
@pytest.fixture(scope="session")
def s3_delta_table_path(s3_bucket, s3_details):
    delta_table_path = f"s3://{s3_bucket}/cli-delta-table"

    storage_options_result = s3_details.to_s3_config()
    assert storage_options_result.is_ok()

    storage_options = storage_options_result.unwrap()
    # logger.error(f"{storage_options=}")

    for i in range(11):
        data = pd.DataFrame({
            "id": [i+2, i+3],
            "name": [f"{str(i)}_name", f"{str(i)}_other_name"]
        })

        write_deltalake(delta_table_path,
                        data,
                        mode="append",
                        storage_options=storage_options
                    )

    # write_deltalake(delta_table_path, data, mode="append", storage_options=storage_options)

    assert delta_table_path
    dt = DeltaTable(delta_table_path, storage_options=storage_options)

    assert dt.version() == 10

    yield delta_table_path


@pytest.fixture(scope="session")
def s3_server():
    host = "localhost"
    port = 5002
    url = "http://{host}:{port}".format(host=host, port=port)
    process = start_service("s3", host, port)
    logger.info(f"{url=}")
    yield url
    requests.post(f"http://{host}:{port}/moto-api/reset")
    stop_process(process)



@pytest.fixture(scope='session')
def s3_bucket(s3_server):

    s3_client = boto3.client('s3', 
                    aws_access_key_id="test-access-key", 
                    aws_secret_access_key="test-secret-key", 
                    region_name="us-east-1",
                    endpoint_url=s3_server
                )
    bucket_name = 'test-bucket'
    s3_client.create_bucket(Bucket=bucket_name)
  
    yield bucket_name

def test_s3_delta_table(s3_bucket, s3_details, s3_delta_table_path):
    storage_options_result = s3_details.to_s3_config()
    assert storage_options_result.is_ok()
    storage_options = storage_options_result.unwrap()
    dt = DeltaTable(s3_delta_table_path, storage_options=storage_options)

    assert dt.version() == 10

def test_s3_compact(s3_bucket, s3_details, s3_delta_table_path):
    dt = DeltaTable(s3_delta_table_path, storage_options=s3_details.to_s3_config().unwrap())
    assert dt.version() == 10
    result = delta_compact(s3_delta_table_path, s3_details)

    assert result.is_ok()
    assert "numFilesAdded" in result.unwrap()

    dt = DeltaTable(s3_delta_table_path, storage_options=s3_details.to_s3_config().unwrap())
    assert dt.version() == 11

def test_s3_vacuum(s3_bucket, s3_details, s3_delta_table_path):
    dt = DeltaTable(s3_delta_table_path, storage_options=s3_details.to_s3_config().unwrap())
    assert dt.version() == 11
    result = delta_vacuum(s3_delta_table_path,
                            s3_details,
                            retention_hours=0,
                            enforce_retention_duration=False,
                            dry_run=False)

    assert result.is_ok()
    assert any(["parquet" in f for f in result.unwrap()])
    assert len(result.unwrap()) == 11

    dt = DeltaTable(s3_delta_table_path, storage_options=s3_details.to_s3_config().unwrap())
    assert dt.version() == 12

def test_s3_create_checkpoint(s3_bucket, s3_details, s3_delta_table_path):
    dt = DeltaTable(s3_delta_table_path, storage_options=s3_details.to_s3_config().unwrap())
    assert dt.version() == 12
    result = delta_create_checkpoint(s3_delta_table_path, s3_details)

    assert result.is_ok()

def test_s3_table_version(s3_bucket, s3_details, s3_delta_table_path):
    dt = DeltaTable(s3_delta_table_path, storage_options=s3_details.to_s3_config().unwrap())
    assert dt.version() == 12
    result = delta_version(s3_delta_table_path, s3_details)
    assert isinstance(result.unwrap(), int)

    assert result.is_ok()
    assert result.unwrap() == 12

    s3_client = boto3.client('s3', 
                aws_access_key_id="test-access-key", 
                aws_secret_access_key="test-secret-key", 
                region_name="us-east-1",
                endpoint_url="http://localhost:5002"
            )
    bucket_name = 'test-bucket'

    files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="cli-delta-table/_delta_log/_last_checkpoint")
    logger.error(f"{files=}")
    assert files["KeyCount"] == 1