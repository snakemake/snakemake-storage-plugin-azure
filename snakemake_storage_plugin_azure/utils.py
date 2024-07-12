import re
from urllib.parse import urlparse


def is_valid_blob_endpoint(endpoint) -> bool:
    """Return True if the endpoint is a valid blob endpoint."""
    blob_endpoint_pattern = re.compile(
        r"^https:\/\/[a-z0-9]+(\.[a-z0-9]+)*\.blob\.core\.windows\.net\/?(.+)?$"
    )
    return bool(blob_endpoint_pattern.match(endpoint))


def is_valid_mock_endpoint(endpoint) -> bool:
    """Return True if the endpoint is a valid mock endpoint."""
    mock_endpoint_pattern1 = re.compile(r"^http:\/\/localhost:\d+\/?(.+)?$")
    mock_endpoint_pattern2 = re.compile(r"^http:\/\/127\.0\.0\.1:\d+\/?(.+)?$")
    return bool(mock_endpoint_pattern1.match(endpoint)) or bool(
        mock_endpoint_pattern2.match(endpoint)
    )


def parse_account_name_from_blob_endpoint_url(endpoint_url):
    """Return the account name from a blob endpoint URL."""
    if not is_valid_blob_endpoint(endpoint_url):
        if not endpoint_url.startswith("https://"):
            raise ValueError("Blob endpoint URL must start with 'https://'")
        raise ValueError(f"Invalid blob endpoint URL: {endpoint_url}")

    parsed = urlparse(endpoint_url)
    return parsed.netloc.split(".")[0]


def parse_account_name_from_mock_endpoint_url(endpoint_url):
    """Return the account name from a mock endpoint URL."""
    parsed = urlparse(endpoint_url)
    return parsed.path.lstrip("/")


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
        print(urlparse(query))
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
