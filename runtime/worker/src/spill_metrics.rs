use datafusion::physical_plan::ExecutionPlan;
use datafusion_execution::TaskContext;
use metrics::{MetricId, RuntimeMetrics};

/// Aggregated DataFusion operator spill metrics for one physical plan tree.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DataFusionSpillMetrics {
    pub spill_count: u64,
    pub spilled_rows: u64,
    pub spilled_bytes: u64,
}

impl DataFusionSpillMetrics {
    fn add_metrics_set(&mut self, metrics: datafusion::physical_plan::metrics::MetricsSet) {
        self.spill_count = self
            .spill_count
            .saturating_add(usize_to_u64(metrics.spill_count().unwrap_or(0)));
        self.spilled_rows = self
            .spilled_rows
            .saturating_add(usize_to_u64(metrics.spilled_rows().unwrap_or(0)));
        self.spilled_bytes = self
            .spilled_bytes
            .saturating_add(usize_to_u64(metrics.spilled_bytes().unwrap_or(0)));
    }

    fn record(self, metrics: RuntimeMetrics) {
        metrics.add(MetricId::WorkerSpillCountTotal, self.spill_count);
        metrics.add(MetricId::WorkerSpilledRowsTotal, self.spilled_rows);
        metrics.add(MetricId::WorkerSpilledBytesTotal, self.spilled_bytes);
    }
}

/// Sum DataFusion spill metrics across a physical plan tree.
pub fn datafusion_spill_metrics(plan: &dyn ExecutionPlan) -> DataFusionSpillMetrics {
    let mut totals = DataFusionSpillMetrics::default();
    collect_datafusion_spill_metrics(plan, &mut totals);
    totals
}

/// Add DataFusion spill metrics from a physical plan tree to pg_fusion metrics.
pub fn record_datafusion_spill_metrics(plan: &dyn ExecutionPlan, metrics: RuntimeMetrics) {
    datafusion_spill_metrics(plan).record(metrics);
}

/// Add any DataFusion spill files/bytes still active after execution to leak counters.
pub fn record_datafusion_spill_leaks(task_ctx: &TaskContext, metrics: RuntimeMetrics) {
    let progress = task_ctx.runtime_env().disk_manager.spilling_progress();
    metrics.add(
        MetricId::WorkerSpillLeakedFilesTotal,
        usize_to_u64(progress.active_files_count),
    );
    metrics.add(
        MetricId::WorkerSpillLeakedBytesTotal,
        progress.current_bytes,
    );
}

fn collect_datafusion_spill_metrics(plan: &dyn ExecutionPlan, totals: &mut DataFusionSpillMetrics) {
    if let Some(metrics) = plan.metrics() {
        totals.add_metrics_set(metrics);
    }
    for child in plan.children() {
        collect_datafusion_spill_metrics(child.as_ref(), totals);
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow_array::{RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use control_transport::{BackendLeaseId, BackendLeaseSlot};
    use datafusion::datasource::MemTable;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionConfig;
    use metrics::{MetricId, RuntimeMetrics, RuntimeMetricsConfig};

    use super::*;
    use crate::{WorkerSpillConfig, WorkerSpillRuntime};

    struct MetricsRegion {
        base: NonNull<u8>,
        layout: std::alloc::Layout,
    }

    impl MetricsRegion {
        fn new() -> (Self, RuntimeMetrics) {
            let config = RuntimeMetricsConfig::new(4).expect("metrics config");
            let layout = RuntimeMetrics::layout(config).expect("metrics layout");
            let alloc_layout =
                std::alloc::Layout::from_size_align(layout.size, layout.align).expect("layout");
            let ptr = unsafe { std::alloc::alloc_zeroed(alloc_layout) };
            let base = NonNull::new(ptr).expect("allocated metrics region");
            let metrics = unsafe { RuntimeMetrics::init_in_place(base, layout.size, config) }
                .expect("metrics init");
            (
                Self {
                    base,
                    layout: alloc_layout,
                },
                metrics,
            )
        }
    }

    impl Drop for MetricsRegion {
        fn drop(&mut self) {
            unsafe { std::alloc::dealloc(self.base.as_ptr(), self.layout) };
        }
    }

    #[test]
    fn records_datafusion_sort_spill_metrics() -> Result<(), Box<dyn std::error::Error>> {
        let root = unique_root("df_sort_metrics");
        let config = WorkerSpillConfig::new(Some(2 * 1024 * 1024), Some(root.clone()));
        let mut spill_runtime = WorkerSpillRuntime::new(config, 10, 20)?;
        let peer = BackendLeaseSlot::new(1, BackendLeaseId::new(2, 3));
        let spill_dir = spill_runtime.execution_dir(peer, 4)?;
        let worker_task_ctx = spill_runtime.task_context(&spill_dir)?;

        let session_config = SessionConfig::new()
            .with_batch_size(100)
            .with_sort_in_place_threshold_bytes(8 * 1024)
            .with_sort_spill_reservation_bytes(32 * 1024);
        let session =
            SessionContext::new_with_config_rt(session_config, worker_task_ctx.runtime_env());
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let partitions = vec![string_batches(200, 100, Arc::clone(&schema))?];
        session.register_table(
            "spill_src",
            Arc::new(MemTable::try_new(Arc::clone(&schema), partitions)?),
        )?;

        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let dataframe = tokio.block_on(session.sql("SELECT s FROM spill_src ORDER BY s"))?;
        let plan = tokio.block_on(dataframe.create_physical_plan())?;
        let result = tokio.block_on(collect(Arc::clone(&plan), session.task_ctx()))?;

        assert_eq!(
            result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            20_000
        );
        let totals = datafusion_spill_metrics(plan.as_ref());
        assert!(totals.spill_count > 0, "expected DataFusion sort spill");
        assert!(totals.spilled_rows > 0, "expected spilled rows");
        assert!(totals.spilled_bytes > 0, "expected spilled bytes");
        let spill_progress = worker_task_ctx
            .runtime_env()
            .disk_manager
            .spilling_progress();
        assert_eq!(spill_progress.active_files_count, 0);
        assert_eq!(spill_progress.current_bytes, 0);

        let (_region, metrics) = MetricsRegion::new();
        record_datafusion_spill_metrics(plan.as_ref(), metrics);
        record_datafusion_spill_leaks(worker_task_ctx.as_ref(), metrics);
        assert_eq!(
            metrics.get(MetricId::WorkerSpillCountTotal),
            totals.spill_count
        );
        assert_eq!(
            metrics.get(MetricId::WorkerSpilledRowsTotal),
            totals.spilled_rows
        );
        assert_eq!(
            metrics.get(MetricId::WorkerSpilledBytesTotal),
            totals.spilled_bytes
        );
        assert_eq!(metrics.get(MetricId::WorkerSpillLeakedFilesTotal), 0);
        assert_eq!(metrics.get(MetricId::WorkerSpillLeakedBytesTotal), 0);

        let dir_path = spill_dir.path().expect("spill dir").to_path_buf();
        spill_dir.cleanup()?;
        assert!(!dir_path.exists());
        assert_eq!(worker_task_ctx.runtime_env().memory_pool.reserved(), 0);
        drop(spill_runtime);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn records_active_spill_file_leaks() -> Result<(), Box<dyn std::error::Error>> {
        let root = unique_root("active_file_leaks");
        let config = WorkerSpillConfig::new(Some(1024 * 1024), Some(root.clone()));
        let mut spill_runtime = WorkerSpillRuntime::new(config, 11, 21)?;
        let peer = BackendLeaseSlot::new(1, BackendLeaseId::new(2, 3));
        let spill_dir = spill_runtime.execution_dir(peer, 4)?;
        let worker_task_ctx = spill_runtime.task_context(&spill_dir)?;
        let file = worker_task_ctx
            .runtime_env()
            .disk_manager
            .create_tmp_file("leak metrics")?;

        let (_region, metrics) = MetricsRegion::new();
        record_datafusion_spill_leaks(worker_task_ctx.as_ref(), metrics);
        assert_eq!(metrics.get(MetricId::WorkerSpillLeakedFilesTotal), 1);
        assert_eq!(metrics.get(MetricId::WorkerSpillLeakedBytesTotal), 0);

        drop(file);
        let dir_path = spill_dir.path().expect("spill dir").to_path_buf();
        spill_dir.cleanup()?;
        assert!(!dir_path.exists());
        drop(spill_runtime);
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    fn string_batches(
        batch_count: usize,
        rows_per_batch: usize,
        schema: Arc<Schema>,
    ) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let mut batches = Vec::with_capacity(batch_count);
        for batch_index in 0..batch_count {
            let values = (0..rows_per_batch)
                .map(|row_index| {
                    let value =
                        batch_count * rows_per_batch - (batch_index * rows_per_batch + row_index);
                    format!("spill-value-{value:020}-abcdefghijklmnopqrstuvwxyz")
                })
                .collect::<Vec<_>>();
            batches.push(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(values))],
            )?);
        }
        Ok(batches)
    }

    fn unique_root(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "pg_fusion_spill_metrics_test_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }
}
