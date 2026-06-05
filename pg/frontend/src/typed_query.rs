use datafusion_common::ScalarValue;

pub use pg_type::{PgConstValue, PgTypeRef};

#[derive(Debug, Clone, PartialEq)]
pub struct TypedQuery {
    pub command: QueryCommand,
    pub relations: Vec<RelationRef>,
    pub values: Vec<ValuesRef>,
    pub ctes: Vec<CteDef>,
    pub cte_refs: Vec<CteRangeRef>,
    pub subqueries: Vec<SubqueryRef>,
    pub from: FromItem,
    pub selection: Option<QueryExpr>,
    pub having: Option<QueryExpr>,
    pub targets: Vec<Target>,
    pub group_refs: Vec<u32>,
    pub grouping_sets: Vec<GroupingSetSpec>,
    pub windows: Vec<WindowSpec>,
    pub set_operation: Option<SetOperationTree>,
    pub sort: Vec<SortKey>,
    pub limit_count: Option<QueryExpr>,
    pub limit_offset: Option<QueryExpr>,
    pub has_aggregates: bool,
    pub has_windows: bool,
    pub has_sublinks: bool,
    pub distinct: DistinctSpec,
    pub has_group_by: bool,
    pub has_having: bool,
    pub has_grouping_sets: bool,
    pub has_set_operations: bool,
    pub has_row_marks: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryCommand {
    Select,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RelationRef {
    pub rtindex: usize,
    pub relid: u32,
    pub schema: String,
    pub name: String,
    pub alias: Option<String>,
    pub columns: Vec<ColumnRef>,
    pub catalog_resolved: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub attnum: i16,
    pub name: String,
    pub pg_type: PgTypeRef,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValuesRef {
    pub rtindex: usize,
    pub alias: Option<String>,
    pub columns: Vec<ColumnRef>,
    pub rows: Vec<Vec<QueryExpr>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CteDef {
    pub id: u64,
    pub name: String,
    pub query: Box<TypedQuery>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CteRangeRef {
    pub rtindex: usize,
    pub cte_id: u64,
    pub name: String,
    pub alias: Option<String>,
    pub columns: Vec<ColumnRef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryRef {
    pub rtindex: usize,
    pub alias: Option<String>,
    pub columns: Vec<ColumnRef>,
    pub query: Box<TypedQuery>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromItem {
    Empty,
    Relation {
        rtindex: usize,
    },
    Values {
        rtindex: usize,
    },
    Cte {
        rtindex: usize,
    },
    Subquery {
        rtindex: usize,
    },
    Join {
        kind: JoinKind,
        left: Box<FromItem>,
        right: Box<FromItem>,
        quals: Option<QueryExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub expr: QueryExpr,
    pub name: Option<String>,
    pub pg_type: PgTypeRef,
    pub resno: i16,
    pub ressortgroupref: u32,
    pub resjunk: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub target_ref: u32,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DistinctSpec {
    None,
    FullRow,
    On { target_refs: Vec<u32> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowSpec {
    pub ref_id: u32,
    pub partition_refs: Vec<u32>,
    pub order: Vec<SortKey>,
    pub frame: WindowFrameSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowFrameSpec {
    pub units: WindowFrameUnits,
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    UnboundedFollowing,
    CurrentRow,
    Preceding(ScalarValue),
    Following(ScalarValue),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetOperator {
    Union,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetOperationTree {
    Range {
        rtindex: usize,
    },
    Operation {
        op: SetOperator,
        all: bool,
        left: Box<SetOperationTree>,
        right: Box<SetOperationTree>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupingSetSpec {
    Empty,
    Simple(Vec<u32>),
    Rollup(Vec<Vec<u32>>),
    Cube(Vec<Vec<u32>>),
    Sets(Vec<GroupingSetSpec>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpr {
    Var(Var),
    OuterVar(OuterVar),
    Const(Const),
    Param(Param),
    RelabelType(Box<QueryExpr>),
    Cast {
        arg: Box<QueryExpr>,
        pg_type: PgTypeRef,
    },
    FunctionCall {
        func: ScalarFunction,
        args: Vec<QueryExpr>,
        pg_type: PgTypeRef,
    },
    Array {
        elements: Vec<QueryExpr>,
        pg_type: PgTypeRef,
    },
    ArraySubscript {
        array: Box<QueryExpr>,
        index: Box<QueryExpr>,
        pg_type: PgTypeRef,
    },
    Bool {
        op: BoolOp,
        args: Vec<QueryExpr>,
    },
    BinaryOp {
        op: QueryOperator,
        left: Box<QueryExpr>,
        right: Box<QueryExpr>,
        pg_type: PgTypeRef,
    },
    UnaryOp {
        op: QueryUnaryOperator,
        arg: Box<QueryExpr>,
        pg_type: PgTypeRef,
    },
    AggregateCall {
        func: AggregateFunction,
        args: Vec<QueryExpr>,
        distinct: bool,
        filter: Option<Box<QueryExpr>>,
        pg_type: PgTypeRef,
    },
    WindowCall {
        func: WindowFunctionKind,
        args: Vec<QueryExpr>,
        winref: u32,
        filter: Option<Box<QueryExpr>>,
        distinct: bool,
        pg_type: PgTypeRef,
    },
    Coalesce {
        args: Vec<QueryExpr>,
        pg_type: PgTypeRef,
    },
    Case {
        operand: Option<Box<QueryExpr>>,
        when_then: Vec<(QueryExpr, QueryExpr)>,
        else_expr: Option<Box<QueryExpr>>,
        pg_type: PgTypeRef,
    },
    ScalarSubquery(Box<TypedQuery>),
    ExistsSubquery {
        subquery: Box<TypedQuery>,
        pg_type: PgTypeRef,
    },
    InSubquery {
        expr: Box<QueryExpr>,
        subquery: Box<TypedQuery>,
        pg_type: PgTypeRef,
    },
    NullTest {
        arg: Box<QueryExpr>,
        is_null: bool,
    },
    BooleanTest {
        arg: Box<QueryExpr>,
        kind: BooleanTestKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    StddevPop,
    StddevSamp,
    VarPop,
    VarSamp,
    RegrCount,
    RegrSxx,
    RegrSyy,
    RegrSxy,
    RegrAvgX,
    RegrAvgY,
    RegrR2,
    RegrSlope,
    RegrIntercept,
    CovarPop,
    CovarSamp,
    Corr,
    StringAgg,
    Grouping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunction {
    Abs,
    Acosh,
    Asinh,
    Atanh,
    Ceil,
    Concat,
    ConcatWs,
    Cosh,
    Exp,
    Floor,
    Format,
    Length,
    Ln,
    NullIf,
    Power,
    QuoteLiteral,
    Random,
    Reverse,
    Round,
    Sinh,
    Sqrt,
    Tanh,
    Trunc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunctionKind {
    Aggregate(AggregateFunction),
    CumeDist,
    DenseRank,
    FirstValue,
    Lag,
    LastValue,
    Lead,
    Ntile,
    NthValue,
    PercentRank,
    Rank,
    RowNumber,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Var {
    pub rtindex: usize,
    pub attnum: i16,
    pub pg_type: PgTypeRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OuterVar {
    pub relation: Option<String>,
    pub name: String,
    pub pg_type: PgTypeRef,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Const {
    pub pg_type: PgTypeRef,
    pub value: Option<PgConstValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Param {
    pub kind: ParamKind,
    pub id: i32,
    pub pg_type: PgTypeRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamKind {
    External,
    Exec,
    Sublink,
    Multiexpr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoolOp {
    And,
    Or,
    Not,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BooleanTestKind {
    IsTrue,
    IsNotTrue,
    IsFalse,
    IsNotFalse,
    IsUnknown,
    IsNotUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOperator {
    Eq,
    NotEq,
    IsDistinctFrom,
    IsNotDistinctFrom,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    BitwiseShiftLeft,
    BitwiseShiftRight,
    StringConcat,
    LikeMatch,
    NotLikeMatch,
    ILikeMatch,
    NotILikeMatch,
    RegexMatch,
    RegexNotMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryUnaryOperator {
    Plus,
    Minus,
}
