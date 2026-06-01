use arrow_schema::{DataType, Field, Schema};
use backend_service::{
    scan_descriptor_matches_for_tests, BackendExecutionState, BackendService, BackendServiceConfig,
    BackendServiceError, ExecutionPlanSource, ExplainInput, PrepareExecutionPlanInput,
};
use datafusion_common::{DFSchema, TableReference};
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan, Union};
use issuance::{IssuanceConfig, IssuancePool, IssuedTx};
use plan_codec::{EncodeProgress, PlanEncodeSession};
use plan_flow::{BackendPlanError, BackendPlanRole, FlowId as PlanFlowId, PlanOpen};
use pool::{PagePool, PagePoolConfig};
use protocol::{
    decode_worker_scan_to_backend, encode_worker_scan_to_backend_into,
    encoded_len_worker_scan_to_backend, ExecutionFailureCode, ProducerDescriptorWire, ProducerRole,
    ScanFlowDescriptor, WorkerScanToBackend, WorkerScanToBackendRef,
};
use scan_flow::{FlowId, ProducerDescriptor, ScanOpen};
use scan_node::{PgCteRefNode, PgScanId, PgScanNode, PgScanSpec};
use scan_sql::{CompiledScan, PgRelation};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::Arc;
use transfer::PageTx;

const TEST_PAGE_SIZE: usize = 4096;
const TEST_PAGE_COUNT: u32 = 4;
const TEST_PERMIT_COUNT: u32 = 4;
const TEST_PLAN_ID: u64 = 1;

struct OwnedRegion {
    base: NonNull<u8>,
    layout: Layout,
}

impl OwnedRegion {
    fn from_size_align(size: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(size, align).expect("region layout");
        let base = unsafe { alloc_zeroed(layout) };
        let base = NonNull::new(base).expect("region allocation");
        Self { base, layout }
    }
}

impl Drop for OwnedRegion {
    fn drop(&mut self) {
        unsafe { dealloc(self.base.as_ptr(), self.layout) };
    }
}

fn opened_plan_role_for_tests(session_epoch: u64) -> (OwnedRegion, OwnedRegion, BackendPlanRole) {
    let page_cfg = PagePoolConfig::new(TEST_PAGE_SIZE, TEST_PAGE_COUNT).expect("page pool config");
    let page_layout = PagePool::layout(page_cfg).expect("page pool layout");
    let page_region = OwnedRegion::from_size_align(page_layout.size, page_layout.align);
    let page_pool =
        unsafe { PagePool::init_in_place(page_region.base, page_layout.size, page_cfg) }
            .expect("page pool");

    let issuance_cfg = IssuanceConfig::new(TEST_PERMIT_COUNT).expect("issuance config");
    let issuance_layout = IssuancePool::layout(issuance_cfg).expect("issuance layout");
    let issuance_region = OwnedRegion::from_size_align(issuance_layout.size, issuance_layout.align);
    let issuance_pool = unsafe {
        IssuancePool::init_in_place(issuance_region.base, issuance_layout.size, issuance_cfg)
    }
    .expect("issuance pool");

    let tx = IssuedTx::new(PageTx::new(page_pool), issuance_pool);
    let mut plan_role = BackendPlanRole::new(tx);
    let plan = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(datafusion_common::DFSchema::empty()),
    });
    let config = BackendServiceConfig::default();
    let plan_open = PlanOpen::new(
        PlanFlowId {
            session_epoch,
            plan_id: TEST_PLAN_ID,
        },
        config.plan_page_kind,
        config.plan_page_flags,
    );
    plan_role.open(plan_open, &plan).expect("plan role open");

    (page_region, issuance_region, plan_role)
}

fn canonical_scan_open() -> ScanOpen {
    ScanOpen::new(
        FlowId {
            session_epoch: 5,
            scan_id: 42,
        },
        0x4152,
        0x0003,
        vec![ProducerDescriptor::leader(0), ProducerDescriptor::worker(1)],
    )
    .expect("canonical scan open")
}

fn encode_plan_for_tests(plan: &LogicalPlan) -> Vec<u8> {
    let mut session = PlanEncodeSession::new(plan).expect("create plan encoder");
    let mut encoded = Vec::new();
    loop {
        let mut chunk = [0_u8; 31];
        match session.write_chunk(&mut chunk).expect("encode plan chunk") {
            EncodeProgress::NeedMoreOutput { written } => {
                assert!(written > 0, "encoder must make progress");
                encoded.extend_from_slice(&chunk[..written]);
            }
            EncodeProgress::Done { written } => {
                encoded.extend_from_slice(&chunk[..written]);
                break;
            }
        }
    }
    encoded
}

fn scan_plan_for_tests(scan_id: PgScanId) -> LogicalPlan {
    let arrow_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let df_schema = DFSchema::try_from_qualified_schema(
        TableReference::partial("public", "encoded_users"),
        &arrow_schema,
    )
    .expect("df schema");
    let compiled = CompiledScan {
        sql: "SELECT \"id\" FROM \"public\".\"encoded_users\"".into(),
        requested_limit: None,
        sql_limit: None,
        selected_columns: vec![0],
        output_columns: vec![0],
        filter_only_columns: Vec::new(),
        residual_filter_columns: Vec::new(),
        pushed_filters: Vec::new(),
        residual_filters: Vec::new(),
        all_filters_compiled: true,
        uses_dummy_projection: false,
    };
    let spec = PgScanSpec::try_new(
        scan_id,
        42,
        PgRelation::new(Some("public"), "encoded_users"),
        &df_schema,
        compiled,
    )
    .expect("scan spec");
    PgScanNode::new(Arc::new(spec)).into_logical_plan()
}

fn encoded_scan_plan_for_tests() -> Vec<u8> {
    let plan = scan_plan_for_tests(PgScanId::new(7));
    encode_plan_for_tests(&plan)
}

fn assert_scan_descriptor_match(
    canonical: &ScanOpen,
    page_kind: u16,
    page_flags: u16,
    producers: &[ProducerDescriptorWire],
    expected: bool,
) {
    let scan = ScanFlowDescriptor::new(page_kind, page_flags, producers).expect("scan descriptor");
    let message = WorkerScanToBackend::OpenScan {
        session_epoch: canonical.flow.session_epoch,
        scan_id: canonical.flow.scan_id,
        scan,
    };
    let mut encoded = vec![0u8; encoded_len_worker_scan_to_backend(message)];
    let written = encode_worker_scan_to_backend_into(message, &mut encoded)
        .expect("encode open scan message");
    let decoded =
        decode_worker_scan_to_backend(&encoded[..written]).expect("decode open scan message");
    let WorkerScanToBackendRef::OpenScan { scan, .. } = decoded else {
        panic!("expected open scan message");
    };

    assert_eq!(scan_descriptor_matches_for_tests(canonical, scan), expected);
}

pub fn future_session_is_rejected_without_active_execution() {
    BackendService::reset_for_tests();

    let err = BackendService::accept_complete_execution(7, 1).unwrap_err();
    assert!(matches!(
        err,
        BackendServiceError::FutureSession {
            current: 0,
            incoming: 1
        }
    ));
}

pub fn stale_session_is_ignored_for_active_execution() {
    BackendService::reset_for_tests();
    BackendService::install_fake_execution_for_tests(3, 5, BackendExecutionState::Running);

    let handled = BackendService::accept_complete_execution(3, 4).unwrap();
    assert!(!handled);
    assert_eq!(BackendService::current_session_epoch_for_tests(), 5);
}

pub fn same_epoch_other_slot_is_ignored() {
    BackendService::reset_for_tests();
    BackendService::install_fake_execution_for_tests(3, 5, BackendExecutionState::Running);

    let handled = BackendService::accept_cancel_execution(9, 5).unwrap();
    assert!(!handled);
    assert_eq!(BackendService::current_session_epoch_for_tests(), 5);
}

pub fn fail_execution_is_accepted_while_starting() {
    BackendService::reset_for_tests();
    BackendService::install_fake_execution_for_tests(3, 5, BackendExecutionState::Starting);

    let handled =
        BackendService::accept_fail_execution(3, 5, ExecutionFailureCode::Internal, Some(11))
            .unwrap();
    assert!(handled);
    assert_eq!(BackendService::current_session_epoch_for_tests(), 5);
    assert!(!BackendService::accept_complete_execution(3, 5).unwrap());
}

pub fn cancel_execution_is_accepted_while_starting() {
    BackendService::reset_for_tests();
    BackendService::install_fake_execution_for_tests(3, 5, BackendExecutionState::Starting);

    let handled = BackendService::accept_cancel_execution(3, 5).unwrap();
    assert!(handled);
    assert_eq!(BackendService::current_session_epoch_for_tests(), 5);
    assert!(!BackendService::accept_complete_execution(3, 5).unwrap());
}

pub fn render_explain_is_rejected_while_execution_is_active() {
    BackendService::reset_for_tests();
    BackendService::install_fake_execution_for_tests(3, 5, BackendExecutionState::Running);

    let err = BackendService::render_explain(ExplainInput {
        plan_source: ExecutionPlanSource::EncodedHybridPlan { bytes: &[] },
        options: Default::default(),
        config: BackendServiceConfig::default(),
        scan_worker_launcher: None,
        actual_scan_parallelism: Default::default(),
    })
    .unwrap_err();
    assert!(matches!(err, BackendServiceError::ExecutionAlreadyActive));
}

pub fn encoded_built_plan_prepares_from_plan_codec_payload() {
    BackendService::reset_for_tests();
    let encoded = encoded_scan_plan_for_tests();

    let prepared = BackendService::prepare_execution_plan(PrepareExecutionPlanInput {
        plan_source: ExecutionPlanSource::EncodedHybridPlan { bytes: &encoded },
        config: BackendServiceConfig::default(),
    })
    .expect("encoded hybrid plan should prepare");

    let schema = prepared.output_schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
}

pub fn encoded_built_plan_deduplicates_repeated_scan_references() {
    BackendService::reset_for_tests();

    let scan_plan = scan_plan_for_tests(PgScanId::new(7));
    let projected_schema = Arc::clone(scan_plan.schema());
    let first_ref = PgCteRefNode::new(
        1,
        "cte",
        scan_plan.clone(),
        Arc::clone(&projected_schema),
        None,
        None,
    )
    .into_logical_plan();
    let second_ref =
        PgCteRefNode::new(1, "cte", scan_plan, projected_schema, None, None).into_logical_plan();
    let plan = LogicalPlan::Union(
        Union::try_new(vec![Arc::new(first_ref), Arc::new(second_ref)]).expect("cte union plan"),
    );
    let encoded = encode_plan_for_tests(&plan);

    let prepared = BackendService::prepare_execution_plan(PrepareExecutionPlanInput {
        plan_source: ExecutionPlanSource::EncodedHybridPlan { bytes: &encoded },
        config: BackendServiceConfig::default(),
    })
    .expect("repeated scan references should prepare");

    assert_eq!(prepared.scan_count_for_tests(), 1);
}

pub fn finalize_execution_start_error_preserves_starting_runtime() {
    BackendService::reset_for_tests();

    let (_page_region, _issuance_region, plan_role) = opened_plan_role_for_tests(5);
    BackendService::install_starting_execution_with_plan_role_for_tests(3, 5, plan_role);

    let err = BackendService::finalize_execution_start().unwrap_err();
    assert!(matches!(
        err,
        BackendServiceError::PlanFlow(BackendPlanError::InvalidState {
            action: "close",
            ..
        })
    ));
    assert!(
        BackendService::step_execution_start().is_ok(),
        "starting runtime must remain installed after finalize failure"
    );
    BackendService::abort_execution_start().unwrap();
    assert!(!BackendService::accept_complete_execution(3, 5).unwrap());
}

pub fn scan_descriptor_matches_accepts_exact_ordered_match() {
    let canonical = canonical_scan_open();

    assert_scan_descriptor_match(
        &canonical,
        canonical.page_kind,
        canonical.page_flags,
        &[
            ProducerDescriptorWire {
                producer_id: 0,
                role: ProducerRole::Leader,
            },
            ProducerDescriptorWire {
                producer_id: 1,
                role: ProducerRole::Worker,
            },
        ],
        true,
    );
}

pub fn scan_descriptor_matches_rejects_page_kind_mismatch() {
    let canonical = canonical_scan_open();

    assert_scan_descriptor_match(
        &canonical,
        canonical.page_kind.wrapping_add(1),
        canonical.page_flags,
        &[
            ProducerDescriptorWire {
                producer_id: 0,
                role: ProducerRole::Leader,
            },
            ProducerDescriptorWire {
                producer_id: 1,
                role: ProducerRole::Worker,
            },
        ],
        false,
    );
}

pub fn scan_descriptor_matches_rejects_page_flags_mismatch() {
    let canonical = canonical_scan_open();

    assert_scan_descriptor_match(
        &canonical,
        canonical.page_kind,
        canonical.page_flags ^ 0x0001,
        &[
            ProducerDescriptorWire {
                producer_id: 0,
                role: ProducerRole::Leader,
            },
            ProducerDescriptorWire {
                producer_id: 1,
                role: ProducerRole::Worker,
            },
        ],
        false,
    );
}

pub fn scan_descriptor_matches_rejects_producer_order_mismatch() {
    let canonical = canonical_scan_open();

    assert_scan_descriptor_match(
        &canonical,
        canonical.page_kind,
        canonical.page_flags,
        &[
            ProducerDescriptorWire {
                producer_id: 1,
                role: ProducerRole::Worker,
            },
            ProducerDescriptorWire {
                producer_id: 0,
                role: ProducerRole::Leader,
            },
        ],
        false,
    );
}

pub fn scan_descriptor_matches_rejects_producer_role_mismatch() {
    let canonical = canonical_scan_open();

    assert_scan_descriptor_match(
        &canonical,
        canonical.page_kind,
        canonical.page_flags,
        &[
            ProducerDescriptorWire {
                producer_id: 0,
                role: ProducerRole::Worker,
            },
            ProducerDescriptorWire {
                producer_id: 1,
                role: ProducerRole::Leader,
            },
        ],
        false,
    );
}

pub fn scan_descriptor_matches_rejects_missing_or_extra_producers() {
    let canonical = canonical_scan_open();

    assert_scan_descriptor_match(
        &canonical,
        canonical.page_kind,
        canonical.page_flags,
        &[ProducerDescriptorWire {
            producer_id: 0,
            role: ProducerRole::Leader,
        }],
        false,
    );

    assert_scan_descriptor_match(
        &canonical,
        canonical.page_kind,
        canonical.page_flags,
        &[
            ProducerDescriptorWire {
                producer_id: 0,
                role: ProducerRole::Leader,
            },
            ProducerDescriptorWire {
                producer_id: 1,
                role: ProducerRole::Worker,
            },
            ProducerDescriptorWire {
                producer_id: 2,
                role: ProducerRole::Worker,
            },
        ],
        false,
    );
}
