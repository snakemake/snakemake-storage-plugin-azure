import os
import uuid
from typing import List, Optional, Type

from conftest import (
    AZURITE_CONNECTION_STRING,
    AZURITE_STORAGE_ACCOUNT,
    AZURITE_TEST_BLOB,
    AZURITE_TEST_CONTAINER,
)
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.tests import TestStorageBase

from snakemake_storage_plugin_azure import StorageProvider, StorageProviderSettings


class TestStorageNoSettings(TestStorageBase):
    __test__ = True
    retrieve_only = True

    def get_query_not_existing(self, tmp_path) -> str:
        container = uuid.uuid4().hex
        path = uuid.uuid4().hex
        return f"az://{AZURITE_STORAGE_ACCOUNT}/{container}/{path}"

    def get_query(self, tmp_path) -> str:
        return (
            f"az://{AZURITE_STORAGE_ACCOUNT}/{AZURITE_TEST_CONTAINER}/"
            f"{AZURITE_TEST_BLOB}"
        )

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        # public dataset storage account and public sas token:
        # https://learn.microsoft.com/en-us/azure/open-datasets/dataset-genomics-data-lake
        os.environ["AZURITE_CONNECTION_STRING"] = AZURITE_CONNECTION_STRING
        return StorageProviderSettings(account_name=AZURITE_STORAGE_ACCOUNT)

    def get_example_args(self) -> List[str]:
        return []
