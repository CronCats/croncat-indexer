use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Transaction::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Transaction::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Transaction::Height)
                            .big_unsigned()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Transaction::Hash).string_len(64).not_null())
                    .col(ColumnDef::new(Transaction::GasWanted).string().null())
                    .col(ColumnDef::new(Transaction::GasUsed).string().null())
                    .col(ColumnDef::new(Transaction::Log).json_binary().null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Transaction::Table).to_owned())
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
enum Transaction {
    Table,
    Id,
    Height,
    Hash,
    GasWanted,
    GasUsed,
    Log,
}
