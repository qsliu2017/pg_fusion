pub use pg_type::{PgConstValue, PgTypeRef};

#[derive(Debug, Clone, PartialEq)]
pub struct TypedQuery {
    pub command: QueryCommand,
    pub relations: Vec<RelationRef>,
    pub from: FromItem,
    pub selection: Option<QueryExpr>,
    pub targets: Vec<Target>,
    pub has_aggregates: bool,
    pub has_windows: bool,
    pub has_sublinks: bool,
    pub has_distinct: bool,
    pub has_group_by: bool,
    pub has_having: bool,
    pub has_grouping_sets: bool,
    pub has_set_operations: bool,
    pub has_limit: bool,
    pub has_sort: bool,
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
pub enum FromItem {
    Relation { rtindex: usize },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub expr: QueryExpr,
    pub name: Option<String>,
    pub pg_type: PgTypeRef,
    pub resno: i16,
    pub resjunk: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryExpr {
    Var(Var),
    Const(Const),
    Param(Param),
    RelabelType(Box<QueryExpr>),
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
    NullTest {
        arg: Box<QueryExpr>,
        is_null: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Var {
    pub rtindex: usize,
    pub attnum: i16,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
}
