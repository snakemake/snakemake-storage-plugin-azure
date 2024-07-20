from snakemake_storage_plugin_azure.utils import (
    container_name_is_valid,
    parse_query_container_name,
    parse_query_path,
)


def test_parse_query_container_name():
    assert (
        parse_query_container_name("az://container/path/example/file.txt")
        == "container"
    )


def test_parse_query_path():
    assert (
        parse_query_path("az://container/path/example/file.txt")
        == "/path/example/file.txt"
    )
    assert parse_query_path("az://container/path/file.txt") == "/path/file.txt"
    assert parse_query_path("az://container/file.txt") == "/file.txt"


def test_container_name_is_valid():
    assert container_name_is_valid("container")
    assert container_name_is_valid("container-test")
    assert not container_name_is_valid("container**notvalid")
