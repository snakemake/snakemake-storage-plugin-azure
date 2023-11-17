# configures the use of azurite mock storage backend
from azure.core.exceptions import HttpResponseError
from azure.storage.blob import BlobClient, BlobServiceClient

AZURITE_STORAGE_ACCOUNT = "devstoreaccount1"
AZURITE_TEST_CONTAINER = "test-container"
AZURITE_TEST_BLOB = "example/test.txt"

AZURITE_MOCK_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)

AZURITE_MOCK_ENDPOINT = f"http://127.0.0.1:10000/{AZURITE_STORAGE_ACCOUNT}"

AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    f"AccountName={AZURITE_STORAGE_ACCOUNT};"
    f"AccountKey={AZURITE_MOCK_KEY};"
    f"BlobEndpoint=http://127.0.0.1:10000/{AZURITE_STORAGE_ACCOUNT};"
)


# bootstrap azurite storage backend for tests
def pytest_generate_tests(metafunc):
    blob_service_client = BlobServiceClient.from_connection_string(
        AZURITE_CONNECTION_STRING
    )
    try:
        blob_service_client.create_container(AZURITE_TEST_CONTAINER)
    except HttpResponseError as e:
        # continue if container exists
        if e.status_code == 409:
            pass
    except Exception as e:
        raise e

    # create a test blob with azurite
    bc: BlobClient = blob_service_client.get_blob_client(
        AZURITE_TEST_CONTAINER, AZURITE_TEST_BLOB
    )
    try:
        bc.upload_blob("Hello, World", overwrite=True)
    except Exception as e:
        raise e
