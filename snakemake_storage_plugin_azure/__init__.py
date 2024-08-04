import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urlparse

from azure.identity import (
    AzureCliCredential,
    ChainedTokenCredential,
    EnvironmentCredential,
    ManagedIdentityCredential,
)
from azure.storage.blob import (
    BlobClient,
    BlobProperties,
    BlobServiceClient,
    ContainerClient,
)
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_storage_plugins.common import Operation
from snakemake_interface_storage_plugins.io import (
    IOCacheStorageInterface,
    Mtime,
    get_constant_prefix,
)
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
    container_name_is_valid,
    parse_query_container_name,
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
            self.bsc: BlobServiceClient = BlobServiceClient.from_connection_string(
                test_credential
            )
        else:
            # prefer azure cli credential,
            # then managed identity,
            # then environment
            credential_chain = (
                AzureCliCredential(),
                ManagedIdentityCredential(),
                EnvironmentCredential(),
            )
            self.bsc = BlobServiceClient(
                endpoint_url, credential=ChainedTokenCredential(*credential_chain)
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
                query="az://container/path/example/file.txt",
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
        if not container_name_is_valid(parsed.netloc):
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="Azure Storage Contianer name must contain only alphanumeric "
                "or dash characters",
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
        self.bsc: BlobServiceClient = self.provider.bsc
        if self.is_valid_query():
            parsed = urlparse(self.query)
            self.container_name = parsed.netloc
            self.blob_path = parsed.path.lstrip("/")
            self._local_suffix = self._local_suffix_from_key(self.blob_path)
            self._is_dir = None

    def container_client(self) -> ContainerClient:
        """Return initialized ContainerClient."""
        return self.bsc.get_container_client(self.container_name)

    def blob_client(self, blob_path=None) -> BlobClient:
        """Return initialized BlobClient."""
        path = blob_path if blob_path else self.blob_path
        return self.bsc.get_blob_client(self.container_name, path)

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

    def is_dir(self):
        """Return True if the query has blobs under it's prefix."""
        if self._is_dir is None:
            self._is_dir = any(self.get_prefix_blobs())
        return self._is_dir

    def get_prefix_blobs(self) -> Iterable[BlobProperties]:
        """Return an iterator of objects in the storage that match the query prefix."""
        prefix = self.blob_path + "/"
        return (
            item
            for item in self.container_client().list_blobs(name_starts_with=prefix)
            if item.name != prefix
        )

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
        """Return True if the object exists, or if the container exists and the path
        is a directory, otherwise false"""
        if not self.container_client().exists():
            return False

        # the blob is a directory
        if self.container_client().exists() and self._is_dir:
            return True

        return self.blob_client().exists()

    @retry_decorator
    def mtime(self) -> float:
        """Returns the modification time."""
        if self.is_dir():
            return max(
                item.last_modified.timestamp() for item in self.get_prefix_blobs()
            )
        return self.blob_client().get_blob_properties().last_modified.timestamp()

    @retry_decorator
    def size(self) -> int:
        """Returns the size in bytes."""
        if self.is_dir():
            return sum(item.size for item in self.get_prefix_blobs())
        return self.blob_client().get_blob_properties().size

    @retry_decorator
    def retrieve_object(self):
        if self.is_dir():
            self.local_path().mkdir(parents=True, exist_ok=True)
            for item in self.get_prefix_blobs():
                name = item.name[len(self.blob_path) :].lstrip("/")
                local_path = self.local_path() / name
                local_path.parent.mkdir(parents=True, exist_ok=True)
                self.download_blob_from_storage(item.name, local_path)
        else:
            self.download_blob_from_storage()

        # Ensure that the object is accessible locally under self.local_path()
        if not self.local_path().exists():
            raise FileNotFoundError(
                f"File {self.local_path()} not found after download."
            )

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.
    @retry_decorator
    def store_object(self):
        """
        Stores the local object in cloud storage.

        If the local object is a directory, the directory is uploaded to the storage.

        If the storage container does not exist, it is created. This check creates the
        dependency that one must provide a credential with container create permissions.
        """
        if not self.container_client().exists():
            self.bsc.create_container(self.container_name)

        if self.local_path().is_dir():
            self._is_dir = True
            for item in self.local_path().rglob("*"):
                if item.is_file():
                    path = Path(self.blob_path / item.relative_to(self.local_path()))
                    self.upload_blob_to_storage(item, path)
        else:
            # Ensure that the object is stored at the location
            # specified by self.local_path().
            if self.local_path().exists():
                self.upload_blob_to_storage(self.local_path(), self.local_path())

    def upload_blob_to_storage(self, local_path: Path = None, remote_path: Path = None):
        """Uploads the file at local_path to blob to storage location remote_path,
        if the file exists, opening a connection and streaming the bytes."""
        if not local_path.exists():
            raise FileNotFoundError(f"File {local_path} not found.")

        with open(str(local_path), "rb") as data:
            self.blob_client(blob_path=str(remote_path)).upload_blob(
                data, overwrite=True
            )

    def download_blob_from_storage(
        self, blob_path: str = None, local_path: Path = None
    ):
        """Downloads the blob from storage,
        opening connection and streaming the bytes."""
        file_path = self.local_path() if local_path is None else local_path
        blob_path = self.blob_path if blob_path is None else blob_path
        with open(str(file_path), "wb") as data:
            data.write(self.blob_client(blob_path=blob_path).download_blob().readall())

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
        prefix = get_constant_prefix(self.query)
        if prefix.startswith(self.container_name):
            prefix = prefix[len(self.container_name) :]
            return (item.key for item in self.get_prefix_blobs(prefix=prefix))
        else:
            raise WorkflowError(
                "S3 storage object {self.query} cannot be used to list matching "
                "objects because bucket name contains a wildcard, which is not "
                "supported."
            )
