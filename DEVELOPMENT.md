## Minimum Requirements

Spirit requires go 1.24 or higher. MySQL version 8.0 and higher is required for performing schema changes.

## Running tests

The tests require a MySQL server to run. If you have MySQL installed locally, you can provide it as an environment variable:

```bash
MYSQL_DSN="root:mypassword@tcp(127.0.0.1:3306)/test" go test -v ./...
```
If the `MYSQL_DSN` is not specified, it currently defaults to `spirit:spirit@tcp(127.0.0.1:8030)/test`. This may change in the future.


### Running tests with docker on specific MySQL version
```bash
cd compose/
docker compose down --volumes && docker compose up -f compose.yml -f 8.0.28.yml 
docker compose up mysql test --abort-on-container-exit
```

## Running linter

```bash
golangci-lint run
```
