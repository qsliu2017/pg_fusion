pub use pg_type::{PgConstValue, PgTypeRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgQuery {
    pub command: PgCommand,
    pub relations: Vec<PgRelationRef>,
    pub from: PgFromItem,
    pub selection: Option<PgExpr>,
    pub targets: Vec<PgTarget>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgCommand {
    Select,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgRelationRef {
    pub rtindex: usize,
    pub relid: u32,
    pub schema: String,
    pub name: String,
    pub alias: Option<String>,
    pub columns: Vec<PgColumnRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgColumnRef {
    pub attnum: i16,
    pub name: String,
    pub pg_type: PgTypeRef,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PgFromItem {
    Relation { rtindex: usize },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgTarget {
    pub expr: PgExpr,
    pub name: Option<String>,
    pub pg_type: PgTypeRef,
    pub resno: i16,
    pub resjunk: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PgExpr {
    Var(PgVar),
    Const(PgConst),
    Param(PgParam),
    RelabelType(Box<PgExpr>),
    Bool {
        op: PgBoolOp,
        args: Vec<PgExpr>,
    },
    BinaryOp {
        op: PgOperator,
        left: Box<PgExpr>,
        right: Box<PgExpr>,
        pg_type: PgTypeRef,
    },
    NullTest {
        arg: Box<PgExpr>,
        is_null: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgVar {
    pub rtindex: usize,
    pub attnum: i16,
    pub pg_type: PgTypeRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PgConst {
    pub pg_type: PgTypeRef,
    pub value: Option<PgConstValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgParam {
    pub kind: PgParamKind,
    pub id: i32,
    pub pg_type: PgTypeRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgParamKind {
    External,
    Exec,
    Sublink,
    Multiexpr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgBoolOp {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PgOperator {
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
