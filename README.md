# Snakemake Storage Plugin Azure

Azure Blob Storage plugin for snakemake.

# Testing

Testing this plugin locally require the azurite storage emulator to be running locally. 
This can be setup using the following docker run command: 

```
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --blobHost 0.0.0.0
```

Then execute the tests: 
```
poetry run coverage run -m pytest tests/tests.py
```

# Example

The below example Snakefile will stream a file, test.txt, containing the text "Hello, World" to the azure blob: https://account.blob.core.windows.net/container/tests.txt

```Snakefile
rule touch:
    output: "account/container/test.txt"
    shell:
        "echo 'Hello, World!' > {output}"
```

**Command:**

```
snakemake --default-storage-provider azure --storage-azure-endpoint-url https://account.blob.core.windows.net --verbose --default-storage-prefix "az://"
```