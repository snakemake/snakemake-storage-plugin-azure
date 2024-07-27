The below example Snakefile and command will stream a file, test.txt, containing the text "Hello, World" to the azure blob: https://accountname.blob.core.windows.net/container/test.txt.

```Snakefile
rule touch:
    output: "test.txt"
    shell:
        "echo 'Hello, World!' > {output}"
```

**Command:**

```
snakemake -j1 \
    --default-storage-provider azure \
    --default-storage-prefix "az://container"
    --storage-azure-account-name accountname \
    --verbose
```