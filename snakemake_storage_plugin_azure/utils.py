from urllib.parse import urlparse


def parse_query_account_name(query: str) -> str:
    """
    Parse the storage account name from the query string

    with format: "az://account/container/path/example/file.txt"

    Args:
        query (str): the azure storage query string.

    Returns:
        str: the storage account name parsed from the query string.
    """
    try:
        account = urlparse(query).netloc
    except Exception as e:
        raise ValueError(
            f"Unable to parse storage account name from query: {query}, {e}"
        )
    return account


def parse_query_container_name(query: str) -> str:
    """
    Parse the storage container from the query string

    with format: "az://account/container/path/example/file.txt"

    Args:
        query (str): the azure storage query string.

    Returns:
        str: the storage container parsed from the query string.
    """
    try:
        parsed = urlparse(query)
        container = parsed.path.split("/")[1]
    except Exception as e:
        raise ValueError(
            f"Unable to parse storage container name from query: {query}, {e}"
        )
    return container


def parse_query_path(query: str) -> str:
    """
    Parse the blob storage path from the query string

    with format: "az://account/container/path/example/file.txt"

    Args:
        query (str): the azure storage query string.

    Returns:
        str: the storage path parsed from the query string (path/example/file.txt).
    """
    try:
        parsed = urlparse(query)
        path = parsed.path.split("/", 2)[-1]
    except Exception as e:
        raise ValueError(f"Unable to parse storage path from query: {query}, {e}")
    return path
