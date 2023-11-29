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
