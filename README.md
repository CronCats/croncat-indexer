&nbsp;

<div align="center">
<img width="600" src="https://github.com/CronCats/croncat-rs/raw/main/croncat.png" />
</div>

&nbsp;

---

# croncat-indexer

Index the chain, get information about croncat contracts and tasks!

## Top Level Database Helpers

See [here](./migration/README.md) for more info on migrations from `sea-orm`.

**(Pro Tip: Make sure you have the env var `DATABASE_URL` set before running)**

### Create a new migration

-   `cargo make generate-migration <name>`

### Migrate database

-   `cargo make migrate`

## Run

-   `cargo run`
