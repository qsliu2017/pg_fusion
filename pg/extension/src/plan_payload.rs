use pg_frontend::{decode_query_ir, encode_query_ir, PgQuery};
use thiserror::Error;

const PAYLOAD_PREFIX: &str = "PGFUSION_CUSTOM_SCAN_V1\n";
const SQL_TAG: &str = "sql\n";
const FRONTEND_TAG: &str = "frontend\n";

#[derive(Debug, Clone)]
pub(crate) enum CustomScanPlanSource {
    SqlText(String),
    FrontendQuery(PgQuery),
}

impl CustomScanPlanSource {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::SqlText(_) => "sql_text",
            Self::FrontendQuery(_) => "pg_frontend",
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum PlanPayloadError {
    #[error("PostgreSQL query IR codec failed: {0}")]
    QueryCodec(#[from] pg_frontend::PgFrontendCodecError),
    #[error("invalid custom scan plan payload: {0}")]
    Invalid(String),
}

pub(crate) fn encode_frontend_query(query: &PgQuery) -> Result<String, PlanPayloadError> {
    let bytes = encode_query_ir(query)?;
    Ok(format!(
        "{PAYLOAD_PREFIX}{FRONTEND_TAG}{}",
        hex_encode(&bytes)
    ))
}

pub(crate) fn encode_sql_text(sql: &str) -> String {
    format!("{PAYLOAD_PREFIX}{SQL_TAG}{}", hex_encode(sql.as_bytes()))
}

pub(crate) fn decode_plan_source(value: &str) -> Result<CustomScanPlanSource, PlanPayloadError> {
    let Some(rest) = value.strip_prefix(PAYLOAD_PREFIX) else {
        return Ok(CustomScanPlanSource::SqlText(value.to_string()));
    };
    if let Some(hex) = rest.strip_prefix(SQL_TAG) {
        let bytes = hex_decode(hex)?;
        let sql = String::from_utf8(bytes)
            .map_err(|err| PlanPayloadError::Invalid(format!("SQL payload is not UTF-8: {err}")))?;
        return Ok(CustomScanPlanSource::SqlText(sql));
    }
    let Some(hex) = rest.strip_prefix(FRONTEND_TAG) else {
        return Err(PlanPayloadError::Invalid(
            "unknown custom scan payload tag".into(),
        ));
    };
    let bytes = hex_decode(hex)?;
    Ok(CustomScanPlanSource::FrontendQuery(decode_query_ir(
        &bytes,
    )?))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn hex_decode(value: &str) -> Result<Vec<u8>, PlanPayloadError> {
    if value.len() % 2 != 0 {
        return Err(PlanPayloadError::Invalid(
            "hex payload has odd length".into(),
        ));
    }
    let mut decoded = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    for chunk in bytes.chunks_exact(2) {
        let high = hex_value(chunk[0])?;
        let low = hex_value(chunk[1])?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

fn hex_value(byte: u8) -> Result<u8, PlanPayloadError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(PlanPayloadError::Invalid(format!(
            "non-hex byte {byte:#x} in payload"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_payload_decodes_as_sql_text() {
        let decoded = decode_plan_source("SELECT 1").unwrap();
        match decoded {
            CustomScanPlanSource::SqlText(sql) => assert_eq!(sql, "SELECT 1"),
            other => panic!("unexpected payload {other:?}"),
        }
    }

    #[test]
    fn tagged_sql_payload_roundtrips() {
        let decoded = decode_plan_source(&encode_sql_text("SELECT 'value'")).unwrap();
        match decoded {
            CustomScanPlanSource::SqlText(sql) => assert_eq!(sql, "SELECT 'value'"),
            other => panic!("unexpected payload {other:?}"),
        }
    }

    #[test]
    fn hex_roundtrips() {
        let bytes = b"\x00hello\xff";
        let encoded = hex_encode(bytes);
        assert_eq!(hex_decode(&encoded).unwrap(), bytes);
    }
}
