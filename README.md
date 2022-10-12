&nbsp;

<div align="center">
<img width="600" src="https://github.com/CronCats/croncat-rs/raw/main/croncat.png" />
</div>

&nbsp;

---

# croncat-indexer

Index the chain, get information about croncat contracts and tasks!

## Run

-   `cargo run`

## Top Level Database Helpers

See [here](./migration/README.md) for more info on migrations from `sea-orm`.

**(Pro Tip: Make sure you have the env var `DATABASE_URL` set before running)**

### Create a new migration

-   `cargo make generate-migration <name>`

### Migrate latest database changes

-   `cargo make migrate-up`

### Rollback latest database changes

-   `cargo make migrate-down`

### Refresh database schema

-   `cargo make migrate-refresh`

### Generate the model from the database

-   `cargo make generate-model`
