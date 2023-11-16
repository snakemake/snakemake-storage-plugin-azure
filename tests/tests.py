import uuid
from typing import Optional, Type
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_storage_plugin_az import StorageProvider, StorageProviderSettings


class TestStorageNoSettings(TestStorageBase):
    __test__ = True
    retrieve_only = True

    def get_query_not_existing(self, tmp_path) -> str:
        container = uuid.uuid4().hex
        path = uuid.uuid4().hex
        return f"az://{container}/{path}"

    def get_query(self, tmp_path) -> str:
        return "az://container/path/test.txt"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        # public dataset storage account and public sas token
        ep = "https://datasetreferencegenomes.blob.core.windows.net/dataset"
        sas = (
            "sv=2019-02-02&se=2050-01-01T08%3A00%3A00Z&",
            "si=prod&sr=c&sig=JtQoPFqiC24GiEB7v9zHLi4RrA2Kd1r%2F3iFt2l9%2FlV8%3D",
        )
        return StorageProviderSettings(endpoint_url=ep, sas_token=sas)
