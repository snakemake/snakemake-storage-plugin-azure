import re
from urllib.parse import urlparse


def parse_query_container_name(query: str) -> str:
    """
    Parse the storage container from the query string

    with format: "az://container/path/example/file.txt"

    Args:
        query (str): the azure storage query string.

    Returns:
        str: the storage container parsed from the query string.
    """
    try:
        parsed = urlparse(query)
        container = parsed.netloc
    except Exception as e:
        raise ValueError(
            f"Unable to parse storage container name from query: {query}, {e}"
        )
    return container


def parse_query_path(query: str) -> str:
    """
    Parse the blob storage path from the query string

    with format: "az://container/path/example/file.txt"

    Args:
        query (str): the azure storage query string.

    Returns:
        str: the storage path parsed from the query string (path/example/file.txt).
    """
    try:
        parsed = urlparse(query)
        path = parsed.path
    except Exception as e:
        raise ValueError(f"Unable to parse storage path from query: {query}, {e}")
    return path


def container_name_is_valid(container_name: str) -> bool:
    """
    Return whether the given container name is valid for this storage provider.
    """

    return re.match("^[\\w-]+$", container_name) is not None
