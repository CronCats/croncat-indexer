use sea_orm_migration::prelude::*;

use crate::m20221011_000001_create_block_table::Block;

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
                    .col(ColumnDef::new(Transaction::BlockId).uuid().not_null())
                    .col(
                        ColumnDef::new(Transaction::Height)
                            .big_unsigned()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Transaction::Hash).string_len(64).not_null())
                    .col(ColumnDef::new(Transaction::Code).integer().not_null())
                    .col(ColumnDef::new(Transaction::GasWanted).string().not_null())
                    .col(ColumnDef::new(Transaction::GasUsed).string().not_null())
                    .col(ColumnDef::new(Transaction::Events).json_binary().not_null())
                    .col(ColumnDef::new(Transaction::Log).text().not_null())
                    .col(ColumnDef::new(Transaction::Info).text().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk-block_id")
                            .from(Transaction::Table, Transaction::BlockId)
                            .to(Block::Table, Block::Id)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
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
    BlockId,
    Height,
    Hash,
    Code,
    GasWanted,
    GasUsed,
    Events,
    Log,
    Info,
}
