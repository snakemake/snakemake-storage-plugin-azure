The below example Snakefile and command will stream a file, test.txt, containing the text "Hello, World" to the azure blob: https://account.blob.core.windows.net/container/test.txt, specified using the default-storage-prefix.

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
    --default-storage-prefix "az://account/container"
    --storage-azure-endpoint-url https://account.blob.core.windows.net \
    --verbose
```