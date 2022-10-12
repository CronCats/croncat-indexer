&nbsp;

<div align="center">
<img width="600" src="https://github.com/CronCats/croncat-rs/raw/main/croncat.png" />
</div>

&nbsp;

---

# croncat-indexer

Index the chain, get information about croncat contracts and tasks!

## Migrate Database

-   `cargo install sea-orm-cli`
-   `DATABASE_URL=<URL> sea-orm-cli migrate refresh`
    -   Example URL: `postgres://postgres:postgres@localhost:5432/croncat_indexer`

## Run

-   `cargo run`
