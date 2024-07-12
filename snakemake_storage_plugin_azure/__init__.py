from dataclasses import dataclass, field
from typing import Iterable, List, Optional
from urllib.parse import urlparse

from azure.core.credentials import AzureSasCredential
from azure.core.exceptions import HttpResponseError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from snakemake_interface_storage_plugins.common import Operation
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface, Mtime
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectGlob,
    StorageObjectRead,
    StorageObjectWrite,
    retry_decorator,
)
from snakemake_interface_storage_plugins.storage_provider import (
    ExampleQuery,
    QueryType,
    StorageProviderBase,
    StorageQueryValidationResult,
)

from snakemake_storage_plugin_azure.utils import (
    is_valid_blob_endpoint,
    is_valid_mock_endpoint,
    parse_account_name_from_blob_endpoint_url,
    parse_account_name_from_mock_endpoint_url,
    parse_query_account_name,
    parse_query_container_name,
    parse_query_path,
)


# Optional:
# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    endpoint_url: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure Blob Storage Account endpoint url",
            # Optionally request that setting is also available for specification
            # via an environment variable. The variable will be named automatically as
            # SNAKEMAKE_<storage-plugin-name>_<param-name>, all upper case.
            # This mechanism should only be used for passwords, usernames, and other
            # credentials.
            # For other items, we rather recommend to let people use a profile
            # for setting defaults
            # (https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles).
            "env_var": False,
            # Optionally specify that setting is required when the executor is in use.
            "required": True,
        },
    )
    access_key: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure Blob Storage Account Access Key Credential."
            "If set, takes precedence over sas_token credential.",
            "env_var": False,
        },
    )
    sas_token: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure Blob Storage Account SAS Token Credential",
            "env_var": False,
        },
    )

    def blob_endpoint_is_valid(endpoint_url: str | None) -> bool:
        """
        Validates the endpoint url.

        Returns True if endpoint_url matches the Azure Blob Storage
        endpoint regex or if endpoint_url matches the local
        azurite storage emulator endpoint used for testing.

        Args:
            endpoint_url (str): The name of the Azure Blob Storage Account endpoint

        Returns:
            bool: True if the endpoint_url is a valid Azure Blob endpoint.
        """
        return is_valid_blob_endpoint(endpoint_url) or is_valid_mock_endpoint(
            endpoint_url
        )

    def set_storage_account_name(self):
        """
        Sets the storage account name

        Sets self.storage_account_name by parsing from the endpoint_url. If the endpoint
        is the local emulator, the parsing is slightly different.

        Raises:
            ValueError: if urlparse fails to parse the endpoint_url or parse the path.
        """
        if is_valid_mock_endpoint(self.endpoint_url):
            self.storage_account_name = parse_account_name_from_mock_endpoint_url(
                self.endpoint_url
            )
        else:
            self.storage_account_name = parse_account_name_from_blob_endpoint_url(
                self.endpoint_url
            )

    def __post_init__(self):
        if self.endpoint_url is not None:
            self.set_storage_account_name()
            if self.access_key:
                self.credential = self.access_key
            elif self.sas_token:
                self.credential = AzureSasCredential(self.sas_token)
            else:
                self.credential = DefaultAzureCredential()


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # further stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.

        self.bsc = BlobServiceClient(
            self.settings.endpoint_url, credential=self.settings.credential
        )

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return False

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        ...

    def rate_limiter_key(self, query: str, operation: Operation):
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        ...

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example query with description for this storage provider."""
        return [
            ExampleQuery(
                query="az://account/container/path/example/file.txt",
                type=QueryType.ANY,
                description="A file in an Azure Blob Storage Account Container",
            )
        ]

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """
        Return whether the given query is valid for this storage provider.

        Args:
            query (str): the storage query string.

        Returns:
            StoryQueryValidationResult: the query validation result describes if the
                query is valid or not, and if not specifies the reason.
        """
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        try:
            print(f"QUERY:{query}")
            parsed = urlparse(query)
        except Exception as e:
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason=f"cannot be parsed as URL ({e})",
            )
        if parsed.scheme != "az":
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="must start with az (az://...)",
            )
        if not parsed.netloc.isalnum:
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="azure storage account name must be strictly alphanumeric",
            )
        return StorageQueryValidationResult(
            query=query,
            valid=True,
        )

    def get_storage_account_name(query: str) -> str:
        """Return the storage account name from the query."""
        return parse_query_account_name(query)

    def get_storage_container_name(self, query: str) -> str:
        """
        Returns the container name from query.
        """
        return parse_query_container_name(query)

    def list_objects(self) -> Iterable[str]:
        """Return an iterator over all objects in the storage that match the query.

        This is optional and can raise a NotImplementedError() instead.
        """
        cc = self.bsc.get_container_client(self.get_storage_container_name())
        return [o for o in cc.list_blob_names()]


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # further stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        if self.is_valid_query():
            self.account_name = parse_query_account_name(self.query)
            self.blob_path = parse_query_path(self.query)
            self.container_name = parse_query_container_name(self.query)
            self._local_suffix = self._local_suffix_from_key(self.blob_path)

            print(f"query: {self.query}")
            print(f"account_name: {self.account_name}")
            print(f"container_name: {self.container_name}")
            print(f"blob_path: {self.blob_path}")
            print(f"local_suffix: {self._local_suffix}")

            # check the storage account parsed form the endpoint_url
            # matches that parsed from the query
            if self.account_name != self.provider.settings.storage_account_name:
                raise ValueError(
                    f"query account name: {self.account_name} must "
                    "match that from endpoint url: "
                    f"{self.provider.settings.storage_account_name}"
                )

    def container(self):
        """Return initialized ContainerClient."""
        try:
            cc: ContainerClient = self.provider.bsc.get_container_client(
                self.container_name
            )
        except Exception as e:
            raise ConnectionError(
                "failed to initialize ContainerClient for container:"
                f" {self.container_name}: {e}"
            )
        return cc

    def blob(self):
        """Return initialized BlobClient."""
        try:
            bc: BlobClient = self.provider.bsc.get_container_client(
                self.container_name
            ).get_blob_client(self.blob_path)
        except Exception as e:
            raise ConnectionError(
                f"failed to initialize BlobClient for blob:" f" {self.blob_path}: {e}"
            )
        return bc

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        # This is optional and can be left as is

        # If this is implemented in a storage object, results have to be stored in
        # the given IOCache object.

        if self.get_inventory_parent():
            # found
            return

        # container exists
        if not self.container_exists():
            cache.exists_in_storage[self.cache_key] = False
        else:
            cache.exists_in_storage[self.cache_key] = True
            for o in self.container().list_blobs():
                key = self.cache_key(self._local_suffix_from_key(o.name))
                cache.mtime[key] = Mtime(storage=o.last_modified.timestamp())
                cache.size[key] = o.size
                cache.exists_remote[key] = True

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return self.cache_key(self.container_name)

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        return self._local_suffix

    def _local_suffix_from_key(self, key: str) -> str:
        return f"{self.container_name}/{key}"

    def cleanup(self):
        # Close any open connections, unmount stuff, etc.
        pass

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        """Return True if the object exists."""
        if not self.container_exists():
            return False
        else:
            return self.blob().exists()

    @retry_decorator
    def mtime(self) -> float:
        """Returns the modification time."""
        return self.blob().get_blob_properties().last_modified.timestamp()

    @retry_decorator
    def size(self) -> int:
        """Returns the size in bytes."""
        return self.blob().get_blob_properties().size

    @retry_decorator
    def retrieve_object(self):
        # Ensure that the object is accessible locally under self.local_path()
        pass

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        """
        Stores the local object in cloud storage.

        If the storage container does not exist, it is created. This check creates the
        dependency that one must provide a credential with container create permissions.
        """

        try:
            if not self.container_exists():
                self.container().create_container(self.container_name)
        # pass on container exists exception
        except Exception as e:
            if e.status_code == 403:
                pass

        # Ensure that the object is stored at the location specified by
        # self.local_path().
        if self.local_path().exists():
            self.upload_blob_to_storage()

    def upload_blob_to_storage(self):
        """Uploads the blob to storage, opening a connection and streaming the bytes."""
        with open(self.local_path(), "rb") as data:
            self.blob().upload_blob(data, overwrite=True)

    @retry_decorator
    def remove(self):
        """Removes the object from blob storage."""
        if self.blob().exists():
            self.blob().delete_blob()

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.
    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        ...

    def container_exists(self) -> bool:
        """Returns True if container exists, False otherwise."""
        try:
            return self.container().exists()
        except HttpResponseError as e:
            if e.status_code == 403:
                raise PermissionError(
                    "the provided credential does not have permission to list "
                    "containers on this storage account"
                )
            else:
                raise e
        except Exception as e:
            raise e
