use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Block::Table)
                    .if_not_exists()
                    .col(ColumnDef::new(Block::Id).uuid().unique_key().not_null())
                    .col(ColumnDef::new(Block::Height).big_unsigned().not_null())
                    .col(ColumnDef::new(Block::Time).timestamp().not_null())
                    .col(ColumnDef::new(Block::ChainId).string_len(32).not_null())
                    .col(ColumnDef::new(Block::Hash).string_len(64).not_null())
                    .col(ColumnDef::new(Block::NumTxs).big_unsigned().not_null())
                    .primary_key(
                        index::Index::create()
                            .col(Block::Height)
                            .col(Block::ChainId),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Block::Table).to_owned())
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
pub enum Block {
    Table,
    Id,
    Height,
    Time,
    ChainId,
    Hash,
    NumTxs,
}
