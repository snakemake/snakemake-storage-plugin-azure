import uuid
from typing import Optional, Type
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_storage_plugin_az import StorageProvider, StorageProviderSettings


class TestStorageNoSettings(TestStorageBase):
    __test__ = True
    retrieve_only = True

    def get_query_not_existing(self) -> str:
        container = uuid.uuid4().hex
        path = uuid.uuid4().hex
        return f"az://{container}/{path}"

    def get_query(self) -> str:
        return "az://container/path/test.txt"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        return StorageProviderSettings(endpoint_url="", access_key="")
