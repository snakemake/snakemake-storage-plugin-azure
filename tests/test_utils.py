from snakemake_storage_plugin_azure.utils import (
    parse_query_account_name,
    parse_query_container_name,
    parse_query_path,
)


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
