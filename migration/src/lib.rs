pub use sea_orm_migration::prelude::*;

mod m20221011_000001_create_block_table;
mod m20221012_141605_create_transaction_table;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20221011_000001_create_block_table::Migration),
            Box::new(m20221012_141605_create_transaction_table::Migration),
        ]
    }
}
