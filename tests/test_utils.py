import pytest
from conftest import AZURITE_MOCK_ENDPOINT

from snakemake_storage_plugin_azure.utils import (
    is_valid_blob_endpoint,
    is_valid_mock_endpoint,
    parse_account_name_from_blob_endpoint_url,
    parse_query_account_name,
    parse_query_container_name,
    parse_query_path,
)


def test_is_valid_blob_endpoint():
    assert is_valid_blob_endpoint("https://acct.blob.core.windows.net") is True
    assert is_valid_blob_endpoint("https://acct.blob.core.windows.net/") is True
    assert (
        is_valid_blob_endpoint("https://acct.blob.core.windows.net/?sas_token") is True
    )
    assert is_valid_blob_endpoint("acct.blob.core.windows.net/") is False
    assert is_valid_blob_endpoint("https://localhost:10000") is False


def test_is_valid_mock_endpoint():
    assert is_valid_mock_endpoint(AZURITE_MOCK_ENDPOINT) is True
    assert is_valid_mock_endpoint("http://localhost:10000") is True


def test_parse_account_name_from_blob_endpoint_url():
    assert (
        parse_account_name_from_blob_endpoint_url("https://acct.blob.core.windows.net")
        == "acct"
    )
    assert (
        parse_account_name_from_blob_endpoint_url("https://acct.blob.core.windows.net/")
        == "acct"
    )
    assert (
        parse_account_name_from_blob_endpoint_url(
            "https://acct.blob.core.windows.net/?sas_token"
        )
        == "acct"
    )
    with pytest.raises(ValueError):
        parse_account_name_from_blob_endpoint_url("acct.blob.core.windows.net/")


def test_parse_query_account_name():
    assert (
        parse_query_account_name("az://acct/container/path/example/file.txt") == "acct"
    )
    assert (
        parse_query_account_name("az://test/container/path/example/file.txt") == "test"
    )


def test_parse_query_container_name():
    assert (
        parse_query_container_name("az://acct/container/path/example/file.txt")
        == "container"
    )


def test_parse_query_path():
    assert (
        parse_query_path("az://acct/container/path/example/file.txt")
        == "path/example/file.txt"
    )
    assert parse_query_path("az://acct/container/path/file.txt") == "path/file.txt"
    assert parse_query_path("az://acct/container/file.txt") == "file.txt"
