#![doc = include_str!("../README.md")]

pub mod error;
pub mod fsm;
pub mod result_pages;
pub mod runtime;
mod runtime_filter_plan;
pub mod scan_exec;
pub mod scan_flow_driver;
pub mod spill;
pub mod spill_metrics;
pub mod transport_scan_source;

pub use control_transport::{BackendLeaseId, BackendLeaseSlot};
pub use error::WorkerRuntimeError;
pub use result_pages::{
    normalize_result_transport_schema, normalize_scan_transport_schema, ResultPageEmitter,
    ResultPageProducer, ResultPageProducerConfig, ResultPageStep,
};
pub use runtime::{
    DecodedInbound, PendingPhysicalPlanning, PhysicalPlanResult, TransportWorkerRuntime,
    WorkerRuntimeConfig, WorkerRuntimeCore, WorkerRuntimeStep,
};
pub use scan_exec::{
    OpenScanRequest, ScanBatchSource, ScanProducerPeer, WorkerPgScanExec, WorkerPgScanExecFactory,
    WorkerScanTuning,
};
pub use scan_flow_driver::{OpenScanControl, ScanFlowDriver, ScanFlowDriverStep, ScanFlowOpen};
pub use spill::{ExecutionSpillDir, WorkerSpillConfig, WorkerSpillRuntime};
pub use spill_metrics::{
    datafusion_spill_metrics, record_datafusion_spill_metrics, DataFusionSpillMetrics,
};
pub use transport_scan_source::{ScanIngressProvider, TransportScanBatchSource};
