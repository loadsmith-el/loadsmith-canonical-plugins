//! In-plugin tabular transforms. The loadsmith engine is EL (no transform stage);
//! these are an *internal* concern of this connector — config-driven steps applied
//! to the fully-materialized [`Table`] before it is emitted as Arrow. The
//! `TransformConfig` `kind` tag is the compiled "registry": [`build`] maps each
//! variant to its `Transform` impl.
//!
//! Applicability is documented, not enforced: transforms are source-agnostic, so
//! all run on both `file` and `list` data. In practice `normalize` and `filter`
//! suit both; `drop-empty-rows` is low-value on lists; and `fix-merged-columns` is
//! a spreadsheet (merged-cell) concept that does not make sense for lists.

pub mod drop_empty;
pub mod filter;
pub mod fix_merged;
pub mod normalize;

use anyhow::Result;

use crate::config::TransformConfig;
use crate::table::Table;

/// A whole-table transform.
pub trait Transform: Send {
    fn apply(&self, table: Table) -> Result<Table>;
}

/// Builds the `Transform` for one config step. `index` is the step's position in
/// the `transform` list, used only to thread a config path into `filter` errors.
pub fn build(cfg: &TransformConfig, index: usize) -> Result<Box<dyn Transform>> {
    Ok(match cfg {
        TransformConfig::Filter { predicate } => {
            let path = format!("transform[{index}].where");
            // Unknown keys were already reported by `configure`.
            Box::new(filter::Filter::new(filter::Predicate::parse(predicate, &path, &mut Vec::new())?))
        }
        TransformConfig::Normalize { columns, filler } => {
            Box::new(normalize::Normalize::new(*columns, filler.clone()))
        }
        TransformConfig::FixMergedColumns { column } => {
            Box::new(fix_merged::FixMergedColumns::new(column.clone()))
        }
        TransformConfig::DropEmptyRows { zero_is_empty } => {
            Box::new(drop_empty::DropEmptyRows::new(*zero_is_empty))
        }
    })
}

/// Builds and applies the full transform chain in order.
pub fn apply_chain(cfgs: &[TransformConfig], mut table: Table) -> Result<Table> {
    for (i, cfg) in cfgs.iter().enumerate() {
        table = build(cfg, i)?.apply(table)?;
    }
    Ok(table)
}
