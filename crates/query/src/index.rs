//! The row-group text index, applied: a `TableProvider` that hands DataFusion a
//! per-file [`ParquetAccessPlan`] built from the manifest's trigram sets.
//!
//! File-level trigram pruning (`select_files`) decides *which files* to open.
//! This decides *which row groups inside them* to read, which is the half that
//! starts mattering after compaction: a 256 MiB file survives file-level pruning
//! whenever any of its ~10M rows mentions the term, and then `ILIKE` reads all of
//! them. The index lets a rare term skip nearly every row group instead.
//!
//! **Why a custom provider rather than `ListingTable`.** DataFusion accepts an
//! externally-computed access plan only as an extension on `PartitionedFile`, and
//! `ListingTable` builds its own `PartitionedFile`s from an object-store listing —
//! there is no hook. So the provider is ours, but everything downstream of
//! `FileScanConfig` is stock DataFusion: same `ParquetFormat::create_physical_plan`,
//! same bloom filters, same predicate pushdown, same `FilterExec` above the scan
//! from returning `Inexact`. The access plan is the only thing added, and
//! DataFusion narrows it further with its own statistics — it never widens it.
//!
//! **The safety contract is the same one as `may_match`**: a skip must be a proof.
//! An access plan is attached only when [`DataFile::row_groups_to_scan`] returns a
//! mask, which requires the sidecar's set count to agree with the manifest's
//! recorded row-group count. Anything else — no index, a legacy file, disagreeing
//! counts — scans the file whole. Slower, never wrong.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DfResult;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use verdigris_core::manifest::{DataFile, Predicate};

/// One file to scan, with the row groups the index says are worth reading.
#[derive(Debug, Clone)]
pub struct IndexedFile {
    /// Object-store key, relative to the store root.
    pub path: String,
    pub bytes: u64,
    /// `None` = scan the whole file. `Some(mask)` = read only where `mask[i]`.
    pub row_groups: Option<Vec<bool>>,
}

impl IndexedFile {
    /// Build the scan plan for `file` under `preds`.
    ///
    /// The mask comes from core — the decision of what is provably matchless is
    /// pure logic over recorded stats and belongs there, next to `may_match`, not
    /// in the DataFusion-specific shell. This function only translates it.
    pub fn plan(file: &DataFile, preds: &[Predicate]) -> Self {
        Self {
            path: file.path.clone(),
            bytes: file.bytes,
            row_groups: file.row_groups_to_scan(preds, file.row_groups as usize),
        }
    }

    /// Scan the whole file — for callers with no predicates to prune on.
    pub fn whole(file: &DataFile) -> Self {
        Self {
            path: file.path.clone(),
            bytes: file.bytes,
            row_groups: None,
        }
    }

    /// Row groups this file will actually read, and how many it has in total.
    /// `(scanned, total)`; a file with no index reports `(total, total)`.
    pub fn scanned_row_groups(&self) -> Option<(usize, usize)> {
        let mask = self.row_groups.as_ref()?;
        Some((mask.iter().filter(|b| **b).count(), mask.len()))
    }

    /// Every row group provably matchless — the file can be dropped entirely.
    ///
    /// Worth checking before building a scan: `select_files` prunes on the
    /// file-level union, which keeps any file where the term's trigrams all appear
    /// *somewhere*, even when no single row group holds all of them.
    fn is_empty(&self) -> bool {
        self.row_groups
            .as_ref()
            .is_some_and(|m| m.iter().all(|b| !*b))
    }

    fn to_partitioned_file(&self) -> PartitionedFile {
        let pf = PartitionedFile::new(self.path.clone(), self.bytes);
        match &self.row_groups {
            // Attach nothing when every row group is to be read: an all-scan plan
            // is what DataFusion defaults to anyway, and not attaching it keeps
            // the length-mismatch failure mode off the table for files the index
            // was never going to help.
            None => pf,
            Some(mask) if mask.iter().all(|b| *b) => pf,
            Some(mask) => {
                let mut plan = ParquetAccessPlan::new_all(mask.len());
                for (i, keep) in mask.iter().enumerate() {
                    if !keep {
                        plan.skip(i);
                    }
                }
                pf.with_extension(plan)
            }
        }
    }
}

/// A Parquet table over an explicit file list, each with an optional row-group
/// access plan.
#[derive(Debug)]
pub struct IndexedParquetTable {
    schema: SchemaRef,
    store_url: ObjectStoreUrl,
    files: Vec<IndexedFile>,
}

impl IndexedParquetTable {
    pub fn new(schema: SchemaRef, store_url: ObjectStoreUrl, files: Vec<IndexedFile>) -> Self {
        // Files whose every row group is provably matchless are dropped here
        // rather than handed to DataFusion as an all-skip plan: opening a file to
        // read none of it is a pointless GET of the footer.
        let files = files.into_iter().filter(|f| !f.is_empty()).collect();
        Self {
            schema,
            store_url,
            files,
        }
    }

    /// Row groups this scan will read vs. how many the indexed files hold, for the
    /// benchmark's "touches < 5% of row groups" claim and for query telemetry.
    /// Files with no index contribute nothing to either side of the ratio.
    pub fn row_group_selectivity(&self) -> (usize, usize) {
        self.files
            .iter()
            .filter_map(|f| f.scanned_row_groups())
            .fold((0, 0), |(s, t), (fs, ft)| (s + fs, t + ft))
    }
}

#[async_trait::async_trait]
impl TableProvider for IndexedParquetTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// `Inexact` for every filter, exactly as `ListingTable` does for a
    /// non-partition column: the scan may over-deliver rows, so DataFusion keeps a
    /// `FilterExec` above it and re-checks. That is what makes the index safe to be
    /// *approximate* — a trigram set can say "maybe" and the filter settles it.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let format = ParquetFormat::default();
        let source = format.file_source(Arc::clone(&self.schema).into());
        let config = FileScanConfigBuilder::new(self.store_url.clone(), source)
            .with_file_group(
                self.files
                    .iter()
                    .map(|f| f.to_partitioned_file())
                    .collect::<Vec<_>>()
                    .into(),
            )
            .with_projection_indices(projection.cloned())?
            .with_limit(limit)
            .build();
        format.create_physical_plan(state, config).await
    }
}
