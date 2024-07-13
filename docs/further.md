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