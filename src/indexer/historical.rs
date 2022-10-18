use std::ops::Deref;

use chrono::NaiveDateTime;
use color_eyre::Result;
use indoc::indoc;
use sea_orm::{DatabaseConnection, DbBackend, FromQueryResult, Statement};
use serde::{Deserialize, Serialize};

///
/// A range of block heights.
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRange(pub (i64, i64));

impl From<(i64, i64)> for BlockRange {
    fn from((start, end): (i64, i64)) -> Self {
        Self((start, end))
    }
}

impl Deref for BlockRange {
    type Target = (i64, i64);

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

///
/// Describe a gap in the block sequence of historical data.
///
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromQueryResult)]
pub struct BlockGap {
    pub start_time: NaiveDateTime,
    pub start: i64,
    pub end: i64,
}

impl BlockGap {
    ///
    /// The SQL query to find gaps in the block sequence.
    ///
    fn query_str() -> &'static str {
        indoc! { r#"
        SELECT start_time,
               height + 1 AS start,
               next_block - 1 AS end
        FROM (
            SELECT time AS start_time,
                   height,
                   lead(height) OVER (ORDER BY height) AS next_block
            FROM   block
            WHERE  chain_id = $1
            AND    time > (NOW() - ($2 || ' day')::INTERVAL)
        ) inner_alias
        WHERE height + 1 <> next_block
        ORDER BY start_time DESC;
        "# }
        .trim()
    }

    ///
    /// Find gaps in the block sequence.
    ///
    async fn query(
        db: &DatabaseConnection,
        chain_id: String,
        lookback_in_days: i64,
    ) -> Result<Vec<Self>> {
        Self::find_by_statement(Statement::from_sql_and_values(
            DbBackend::Postgres,
            Self::query_str(),
            vec![chain_id.into(), lookback_in_days.to_string().into()],
        ))
        .all(db)
        .await
        .map_err(|err| err.into())
    }
}

///
/// Implement [`Iterator`] for [`BlockRange`].
///
impl Iterator for BlockGap {
    type Item = BlockRange;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start <= self.end {
            let start = self.start;
            let end = self.end;
            self.start += 1;
            Some((start, end).into())
        } else {
            None
        }
    }
}

///
/// Get historical gaps in the block sequence for a chain within a lookback period (interval is days for now).
///
pub async fn get_block_gaps(
    db: &DatabaseConnection,
    chain_id: String,
    lookback_in_days: i64,
) -> Result<Vec<BlockGap>> {
    BlockGap::query(db, chain_id, lookback_in_days).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_gap_iterator() {
        let mut block_gap = BlockGap {
            start_time: NaiveDateTime::from_timestamp(0, 0),
            start: 1,
            end: 3,
        };
        assert_eq!(block_gap.next(), Some((1, 3).into()));
        assert_eq!(block_gap.next(), Some((2, 3).into()));
        assert_eq!(block_gap.next(), Some((3, 3).into()));
        assert_eq!(block_gap.next(), None);
    }
}
