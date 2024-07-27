# Snakemake Storage Plugin Azure

Azure Blob Storage plugin for snakemake. For documentation and usage instructions, see the [Snakemake Plugin Catalog](https://snakemake.github.io/snakemake-plugin-catalog/plugins/storage/azure.html).

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

The below example Snakefile and command will stream a file, test.txt, containing the text "Hello, World" to the azure blob: https://accountname.blob.core.windows.net/container/test.txt

```Snakefile
rule touch:
    output: "test.txt"
    shell:
        "echo 'Hello, World!' > {output}"
```

**Command:**

The storage account and container that the output file is streamed to is specified using the default-storage-prefix.

```
snakemake -j1 \
    --default-storage-provider azure \
    --default-storage-prefix "az://container"
    --storage-azure-account-name accountname \
    --verbose
```
