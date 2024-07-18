import os
from dataclasses import dataclass, field
from typing import Iterable, List, Optional
from urllib.parse import urlparse

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
    account_name: Optional[str] = field(
        default=None,
        metadata={
            "help": "Azure Blob Storage Account name",
            # Optionally request that setting is also available for specification
            # via an environment variable. The variable will be named automatically as
            # SNAKEMAKE_<storage-plugin-name>_<param-name>, all upper case.
            # This mechanism should only be used for passwords, usernames, and other
            # credentials.
            # For other items, we rather recommend to let people use a profile
            # for setting defaults
            # (https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles).
            "env_var": True,
            # Optionally specify that setting is required when the executor is in use.
            "required": True,
        },
    )


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
        endpoint_url = f"https://{self.settings.account_name}.blob.core.windows.net"

        # use mock storage credential for tests
        test_credential = os.getenv("AZURITE_CONNECTION_STRING")
        if test_credential:
            self.blob_account_client = BlobServiceClient.from_connection_string(
                test_credential
            )
        else:
            self.blob_account_client = BlobServiceClient(
                endpoint_url, credential=DefaultAzureCredential()
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

    def get_storage_container_name(self, query: str) -> str:
        """
        Returns the container name from query.
        """
        return parse_query_container_name(query)

    def list_objects(self) -> Iterable[str]:
        """Return an iterator over all objects in the storage that match the query.

        This is optional and can raise a NotImplementedError() instead.
        """
        cc = self.blob_account_client.get_container_client(
            self.get_storage_container_name()
        )
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
        self.blob_account_client: BlobServiceClient = self.provider.blob_account_client
        if self.is_valid_query():
            self.account_name = parse_query_account_name(self.query)
            self.blob_path = parse_query_path(self.query)
            self.container_name = parse_query_container_name(self.query)
            self._local_suffix = self._local_suffix_from_key(self.blob_path)

            # check the storage account parsed form the endpoint_url
            # matches that parsed from the query
            if self.account_name != self.provider.settings.account_name:
                raise ValueError(
                    f"query account name: {self.account_name} must "
                    "match that from endpoint url: "
                    f"{self.provider.settings.account_name}"
                )

    def container_client(self) -> ContainerClient:
        """Return initialized ContainerClient."""
        return self.blob_account_client.get_container_client(self.container_name)

    def blob_client(self) -> BlobClient:
        """Return initialized BlobClient."""
        return self.blob_account_client.get_blob_client(
            self.container_name, self.blob_path
        )

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
        if not self.container_client().exists():
            cache.exists_in_storage[self.cache_key] = False
        else:
            cache.exists_in_storage[self.cache_key] = True
            for o in self.container_client().list_blobs():
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
        if not self.container_client().exists():
            return False
        else:
            return self.blob_client().exists()

    @retry_decorator
    def mtime(self) -> float:
        """Returns the modification time."""
        return self.blob_client().get_blob_properties().last_modified.timestamp()

    @retry_decorator
    def size(self) -> int:
        """Returns the size in bytes."""
        return self.blob_client().get_blob_properties().size

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
        if not self.container_client().exists():
            self.blob_account_client.create_container(self.container_name)

        # Ensure that the object is stored at the location
        # specified by self.local_path().
        if self.local_path().exists():
            self.upload_blob_to_storage()

    def upload_blob_to_storage(self):
        """Uploads the blob to storage, opening a connection and streaming the bytes."""
        with open(self.local_path(), "rb") as data:
            self.blob_client().upload_blob(data, overwrite=True)

    @retry_decorator
    def remove(self):
        """Removes the object from blob storage."""
        if self.blob_client().exists():
            self.blob_client().delete_blob()

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.
    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        ...
