use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::apply::{LargeObjectDone, LargeObjectTxn};
use crate::contracts::{Error, ErrorKind, IoControl, RestorePolicy, Result};
use crate::pg::{PgConnection, PgParam, PgResult, TransactionStatus};

pub(crate) const INVENTORY_PAGE: usize = 128;
const METADATA_BYTES: usize = 65_536;
const CHUNK_BYTES: usize = 1024 * 1024;
const INV_READ: u32 = 0x40000;
const INV_WRITE: u32 = 0x20000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LargeObject {
    pub(crate) oid: u32,
    pub(crate) source_metadata: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LargeObjectMeasurement {
    pub(crate) target_owner: u32,
    pub(crate) target_acl: Option<String>,
    pub(crate) target_comment: Option<String>,
    pub(crate) source_metadata: String,
    pub(crate) logical_bytes: u64,
    pub(crate) content_hash: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SelectedMetadata {
    #[serde(deserialize_with = "Option::deserialize")]
    owner: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    acl: Option<AclDelta>,
    #[serde(deserialize_with = "Option::deserialize")]
    comment: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AclDelta {
    source_owner: String,
    revoke_owner_default: bool,
    grants: Vec<Grant>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
struct Grant {
    grantor: String,
    #[serde(deserialize_with = "Option::deserialize")]
    grantee: Option<String>,
    privilege: String,
    grantable: bool,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct ActualMetadata {
    owner: u32,
    acl: Option<String>,
    comment: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_hex_is_bounded_and_rejects_invalid_server_data() {
        assert_eq!(decode_hex(b"00ff275c").unwrap(), [0, 255, 39, 92]);
        assert_eq!(encode_hex(&[0, 255, 39, 92]).unwrap(), "00ff275c");
        for bytes in [b"0".as_slice(), b"fg", b"FF"] {
            assert!(decode_hex(bytes).is_err());
        }
        assert!(decode_hex(&vec![b'0'; CHUNK_BYTES * 2 + 2]).is_err());
        assert!(encode_hex(&vec![0; CHUNK_BYTES + 1]).is_err());
    }

    #[test]
    fn sha256_matches_known_vectors_and_chunk_boundaries() {
        for (input, expected) in [
            (
                b"".as_slice(),
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
            (
                b"abc".as_slice(),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            ),
        ] {
            let mut digest = Sha256::new();
            for byte in input {
                digest.update([*byte]);
            }
            assert_eq!(encode_hex(&digest.finalize()).unwrap(), expected);
        }
        let mut digest = Sha256::new();
        for _ in 0..1000 {
            digest.update([b'a'; 1000]);
        }
        assert_eq!(
            encode_hex(&digest.finalize()).unwrap(),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn selected_metadata_is_canonical_bounded_and_validated() {
        let policy = RestorePolicy::default();
        let metadata = r#"{"owner":"owner","acl":{"source_owner":"owner","revoke_owner_default":false,"grants":[{"grantor":"owner","grantee":null,"privilege":"SELECT","grantable":false}]},"comment":null}"#;
        assert_eq!(
            canonical_metadata(metadata.as_bytes(), &policy).unwrap(),
            metadata
        );
        assert!(canonical_metadata(b"{}", &policy).is_err());
        assert!(canonical_metadata(&vec![b' '; METADATA_BYTES + 1], &policy).is_err());
        assert!(
            canonical_metadata(metadata.replace("SELECT", "DELETE").as_bytes(), &policy).is_err()
        );
    }
    #[test]
    fn acl_delta_matches_restore_owner_and_no_owner_semantics() {
        let grant = |grantor: &str, grantee: &str, privilege: &str, grantable| Grant {
            grantor: grantor.into(),
            grantee: Some(grantee.into()),
            privilege: privilege.into(),
            grantable,
        };
        let empty = AclDelta {
            source_owner: "source".into(),
            revoke_owner_default: true,
            grants: vec![],
        };
        assert!(expected_acl(Some(&empty), "source").is_empty());
        assert_eq!(
            expected_acl(Some(&empty), "target"),
            expected_acl(None, "target")
        );
        let select_only = AclDelta {
            grants: vec![grant("source", "source", "SELECT", false)],
            ..empty.clone()
        };
        assert_eq!(
            expected_acl(Some(&select_only), "source"),
            vec![grant("source", "source", "SELECT", false)]
        );
        let expected = expected_acl(Some(&select_only), "target");
        assert_eq!(expected.len(), 3);
        assert!(expected.contains(&grant("target", "source", "SELECT", false)));
        let chain = AclDelta {
            source_owner: "source".into(),
            revoke_owner_default: false,
            grants: vec![
                grant("source", "a", "SELECT", true),
                grant("a", "b", "SELECT", false),
            ],
        };
        let expected = expected_acl(Some(&chain), "target");
        assert!(expected.contains(&grant("target", "a", "SELECT", true)));
        assert!(expected.contains(&grant("a", "b", "SELECT", false)));
        assert_eq!(chain.grants[0].grantor, "source");
        assert_eq!(chain.grants[1].grantor, "a");
    }

    #[test]
    fn public_acl_recipient_is_distinct_from_a_role_named_public() {
        let mut public = Grant {
            grantor: "owner".into(),
            grantee: None,
            privilege: "SELECT".into(),
            grantable: false,
        };
        let named = Grant {
            grantee: Some("PUBLIC".into()),
            ..public.clone()
        };
        assert_ne!(public, named);
        public.grantee = Some("owner".into());
        assert_ne!(public, named);
    }

    #[test]
    fn aggregate_counts_refuse_object_and_byte_overflow() {
        let receipt = LargeObjectDone {
            generation: crate::contracts::CopyGeneration(1),
            object_oid: 7,
            target_owner: 10,
            target_acl: None,
            target_comment: None,
            source_metadata: "{}".into(),
            logical_bytes: 1,
            content_hash: [0; 32],
            sync_phase: crate::apply::LargeObjectPhase::Initial,
        };
        let mut completion = crate::contracts::LargeObjectCompletion {
            objects: u64::MAX,
            bytes: 0,
        };
        assert!(add_completion(&mut completion, &receipt).is_err());
        let mut completion = crate::contracts::LargeObjectCompletion {
            objects: 0,
            bytes: u64::MAX,
        };
        assert!(add_completion(&mut completion, &receipt).is_err());
        let mut completion = crate::contracts::LargeObjectCompletion {
            objects: 0,
            bytes: 0,
        };
        add_completion(&mut completion, &receipt).unwrap();
        assert_eq!(
            completion,
            crate::contracts::LargeObjectCompletion {
                objects: 1,
                bytes: 1
            }
        );
    }

    #[test]
    fn chunked_zero_holes_hash_as_logical_bytes() {
        let mut sparse = Sha256::new();
        sparse.update(b"before");
        for _ in 0..1024 {
            sparse.update([0; 1024]);
        }
        sparse.update(b"after");
        let mut explicit = Sha256::new();
        explicit.update(b"before");
        explicit.update(vec![0; CHUNK_BYTES]);
        explicit.update(b"after");
        assert_eq!(sparse.finalize(), explicit.finalize());
    }
}

fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

fn require_transaction(connection: &PgConnection) -> Result<()> {
    if connection.transaction_status()? != TransactionStatus::InTransaction {
        return Err(refusal("large-objects: an active transaction is required"));
    }
    Ok(())
}

fn text(result: &PgResult, row: usize, column: usize) -> Result<&str> {
    std::str::from_utf8(
        result
            .cell(row, column)?
            .ok_or_else(|| refusal("large-objects: required catalog value is absent"))?,
    )
    .map_err(|_| refusal("large-objects: invalid catalog text"))
}

fn canonical_metadata(bytes: &[u8], policy: &RestorePolicy) -> Result<String> {
    if bytes.len() > METADATA_BYTES {
        return Err(refusal(
            "large-objects: metadata exceeds its bounded budget",
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|_| refusal("large-objects: malformed selected metadata"))?;
    if value.as_object().is_none_or(|fields| {
        fields.len() != 3
            || !["owner", "acl", "comment"]
                .iter()
                .all(|key| fields.contains_key(*key))
    }) {
        return Err(refusal(
            "large-objects: selected metadata fields are absent",
        ));
    }
    let metadata: SelectedMetadata = serde_json::from_value(value)
        .map_err(|_| refusal("large-objects: malformed selected metadata"))?;
    if (!policy.no_owner && metadata.owner.as_ref().is_none_or(|owner| owner.is_empty()))
        || (policy.no_owner && metadata.owner.is_some())
        || (policy.no_acl && metadata.acl.is_some())
        || (policy.no_comments && metadata.comment.is_some())
    {
        return Err(refusal(
            "large-objects: selected metadata disagrees with restore policy",
        ));
    }
    if let Some(acl) = &metadata.acl {
        let mut seen = std::collections::BTreeSet::new();
        if acl.source_owner.is_empty()
            || acl.grants.iter().any(|grant| {
                grant.grantor.is_empty()
                    || grant.grantee.as_ref().is_some_and(|role| role.is_empty())
                    || !matches!(grant.privilege.as_str(), "SELECT" | "UPDATE")
                    || !seen.insert((&grant.grantor, &grant.grantee, &grant.privilege))
            })
        {
            return Err(refusal("large-objects: invalid large object ACL delta"));
        }
    }
    let canonical = serde_json::to_string(&metadata)
        .map_err(|_| refusal("large-objects: selected metadata cannot be encoded"))?;
    if canonical.len() > METADATA_BYTES {
        return Err(refusal(
            "large-objects: canonical metadata exceeds its budget",
        ));
    }
    Ok(canonical)
}

fn metadata_query(after_oid: u32, exact: bool, policy: &RestorePolicy) -> String {
    let owner = if policy.no_owner {
        "NULL::pg_catalog.text"
    } else {
        "pg_catalog.pg_get_userbyid(m.lomowner)"
    };
    let acl = if policy.no_acl {
        "NULL::pg_catalog.json"
    } else {
        "CASE WHEN m.lomacl IS NULL THEN NULL::pg_catalog.json ELSE pg_catalog.json_build_object('source_owner',pg_catalog.pg_get_userbyid(m.lomowner),'revoke_owner_default',NOT(m.lomacl @> ARRAY[pg_catalog.makeaclitem(m.lomowner,m.lomowner,'SELECT,UPDATE',false)]),'grants',(SELECT COALESCE(pg_catalog.json_agg(pg_catalog.json_build_object('grantor',pg_catalog.pg_get_userbyid(a.grantor),'grantee',CASE WHEN a.grantee=0 THEN NULL ELSE pg_catalog.pg_get_userbyid(a.grantee) END,'privilege',a.privilege_type,'grantable',a.is_grantable) ORDER BY CASE WHEN a.grantor=m.lomowner AND a.grantee=m.lomowner THEN 0 ELSE 1 END,a.ordinality),'[]'::pg_catalog.json) FROM pg_catalog.aclexplode(m.lomacl) WITH ORDINALITY AS a WHERE NOT(a.grantor=m.lomowner AND a.grantee=m.lomowner AND m.lomacl @> ARRAY[pg_catalog.makeaclitem(m.lomowner,m.lomowner,'SELECT,UPDATE',false)]))) END"
    };
    let comment = if policy.no_comments {
        "NULL::pg_catalog.text"
    } else {
        "pg_catalog.obj_description(m.oid,'pg_largeobject')"
    };
    let predicate = if exact { "=" } else { ">" };
    format!(
        "SELECT oid, CASE WHEN pg_catalog.octet_length(selected)+pg_catalog.octet_length(actual)<=65536 THEN selected END, CASE WHEN pg_catalog.octet_length(selected)+pg_catalog.octet_length(actual)<=65536 THEN actual END FROM (SELECT m.oid, pg_catalog.json_build_object('owner',{owner},'acl',{acl},'comment',{comment})::pg_catalog.text AS selected, pg_catalog.json_build_object('owner',m.lomowner,'acl',m.lomacl::pg_catalog.text,'comment',pg_catalog.obj_description(m.oid,'pg_largeobject'))::pg_catalog.text AS actual FROM pg_catalog.pg_largeobject_metadata AS m WHERE m.oid {predicate} {after_oid}::pg_catalog.oid ORDER BY m.oid LIMIT 128) AS bounded ORDER BY oid"
    )
}

pub(crate) fn inspect_page(
    connection: &mut PgConnection,
    after_oid: u32,
    policy: &RestorePolicy,
    io: &IoControl,
) -> Result<Vec<LargeObject>> {
    require_transaction(connection)?;
    let result = connection.query(&metadata_query(after_oid, false, policy), io)?;
    if result.columns()? != 3 || result.rows() > INVENTORY_PAGE {
        return Err(refusal("large-objects: invalid inventory page"));
    }
    let mut page = Vec::with_capacity(result.rows());
    let mut previous = after_oid;
    for row in 0..result.rows() {
        let oid = text(&result, row, 0)?
            .parse::<u32>()
            .map_err(|_| refusal("large-objects: invalid inventory OID"))?;
        if oid <= previous {
            return Err(refusal(
                "large-objects: inventory is not strictly increasing",
            ));
        }
        previous = oid;
        let source_metadata = canonical_metadata(text(&result, row, 1)?.as_bytes(), policy)?;
        actual_metadata(text(&result, row, 2)?.as_bytes())?;
        page.push(LargeObject {
            oid,
            source_metadata,
        });
    }
    Ok(page)
}

fn actual_metadata(bytes: &[u8]) -> Result<ActualMetadata> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Metadata {
        owner: u32,
        acl: Option<String>,
        comment: Option<String>,
    }
    if bytes.len() > METADATA_BYTES {
        return Err(refusal("large-objects: actual metadata exceeds its budget"));
    }
    let value: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|_| refusal("large-objects: malformed actual metadata"))?;
    if value.as_object().is_none_or(|fields| {
        fields.len() != 3
            || !["owner", "acl", "comment"]
                .iter()
                .all(|key| fields.contains_key(*key))
    }) {
        return Err(refusal("large-objects: actual metadata fields are absent"));
    }
    let metadata: Metadata = serde_json::from_value(value)
        .map_err(|_| refusal("large-objects: malformed actual metadata"))?;
    if metadata.owner == 0 {
        return Err(refusal("large-objects: actual object owner is absent"));
    }
    Ok(ActualMetadata {
        owner: metadata.owner,
        acl: metadata.acl,
        comment: metadata.comment,
    })
}

fn inspect_exact(
    connection: &mut PgConnection,
    oid: u32,
    policy: &RestorePolicy,
    io: &IoControl,
) -> Result<(String, ActualMetadata)> {
    if oid == 0 {
        return Err(refusal("large-objects: zero object OID is invalid"));
    }
    let result = connection.query(&metadata_query(oid, true, policy), io)?;
    if result.rows() != 1 || result.columns()? != 3 || text(&result, 0, 0)? != oid.to_string() {
        return Err(refusal("large-objects: exact object metadata is missing"));
    }
    Ok((
        canonical_metadata(text(&result, 0, 1)?.as_bytes(), policy)?,
        actual_metadata(text(&result, 0, 2)?.as_bytes())?,
    ))
}

fn scalar(connection: &mut PgConnection, sql: &str, io: &IoControl) -> Result<u64> {
    let result = connection.query(sql, io)?;
    if result.rows() != 1 || result.columns()? != 1 {
        return Err(refusal("large-objects: unexpected function result"));
    }
    text(&result, 0, 0)?
        .parse()
        .map_err(|_| refusal("large-objects: invalid function result"))
}

fn open(connection: &mut PgConnection, oid: u32, mode: u32, io: &IoControl) -> Result<u32> {
    require_transaction(connection)?;
    scalar(
        connection,
        &format!("SELECT pg_catalog.lo_open({oid}::pg_catalog.oid,{mode})"),
        io,
    )?
    .try_into()
    .map_err(|_| refusal("large-objects: invalid descriptor"))
}

fn close(connection: &mut PgConnection, descriptor: u32, io: &IoControl) -> Result<()> {
    if scalar(
        connection,
        &format!("SELECT pg_catalog.lo_close({descriptor})"),
        io,
    )? != 0
    {
        return Err(refusal("large-objects: descriptor did not close"));
    }
    Ok(())
}

fn encode_hex(bytes: &[u8]) -> Result<String> {
    if bytes.len() > CHUNK_BYTES {
        return Err(refusal("large-objects: content chunk exceeds its budget"));
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 15) as usize] as char);
    }
    Ok(encoded)
}

fn decode_hex(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() > CHUNK_BYTES * 2 || !bytes.len().is_multiple_of(2) {
        return Err(refusal(
            "large-objects: invalid bounded hexadecimal content",
        ));
    }
    let digit = |byte| match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(refusal("large-objects: invalid hexadecimal content")),
    };
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Ok((digit(pair[0])? << 4) | digit(pair[1])?))
        .collect()
}

fn read_chunk(connection: &mut PgConnection, descriptor: u32, io: &IoControl) -> Result<Vec<u8>> {
    let result = connection.query(
        &format!("SELECT pg_catalog.encode(pg_catalog.loread({descriptor},{CHUNK_BYTES}),'hex')"),
        io,
    )?;
    if result.rows() != 1 || result.columns()? != 1 {
        return Err(refusal("large-objects: invalid bounded read result"));
    }
    decode_hex(text(&result, 0, 0)?.as_bytes())
}

fn fingerprint(connection: &mut PgConnection, oid: u32, io: &IoControl) -> Result<(u64, [u8; 32])> {
    let descriptor = open(connection, oid, INV_READ, io)?;
    let mut bytes = 0u64;
    let mut digest = Sha256::new();
    loop {
        let chunk = read_chunk(connection, descriptor, io)?;
        if chunk.is_empty() {
            break;
        }
        bytes = bytes
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| refusal("large-objects: logical byte count overflow"))?;
        digest.update(&chunk);
    }
    close(connection, descriptor, io)?;
    Ok((bytes, digest.finalize().into()))
}

pub(crate) fn validate_owned(
    connection: &mut PgConnection,
    receipt: &LargeObjectDone,
    io: &IoControl,
) -> Result<()> {
    require_transaction(connection)?;
    let policy = RestorePolicy {
        no_owner: true,
        no_acl: true,
        no_comments: true,
        no_tablespaces: true,
    };
    let (_, actual) = inspect_exact(connection, receipt.object_oid, &policy, io)?;
    if actual.owner != receipt.target_owner
        || actual.acl != receipt.target_acl
        || actual.comment != receipt.target_comment
    {
        return Err(refusal("large-objects: owned object metadata drifted"));
    }
    let (bytes, hash) = fingerprint(connection, receipt.object_oid, io)?;
    if bytes != receipt.logical_bytes || hash != receipt.content_hash {
        return Err(refusal("large-objects: owned object content drifted"));
    }
    Ok(())
}

pub(crate) fn compare(
    source: &mut PgConnection,
    target: &mut PgConnection,
    policy: &RestorePolicy,
    io: &IoControl,
) -> Result<()> {
    require_transaction(source)?;
    require_transaction(target)?;
    let mut after = 0;
    loop {
        let source_page = inspect_page(source, after, policy, io)?;
        let target_page = inspect_page(target, after, policy, io)?;
        if source_page
            .iter()
            .map(|object| object.oid)
            .ne(target_page.iter().map(|object| object.oid))
        {
            return Err(refusal(
                "large-objects: source and target inventories differ",
            ));
        }
        if source_page.is_empty() {
            return Ok(());
        }
        for object in source_page {
            verify_selected_metadata(target, object.oid, &object.source_metadata, policy, io)?;
            if fingerprint(source, object.oid, io)? != fingerprint(target, object.oid, io)? {
                return Err(refusal(
                    "large-objects: source and target logical content differs",
                ));
            }
            after = object.oid;
        }
    }
}

fn run_page(
    options: &crate::contracts::CloneOptions,
    ticket: &crate::contracts::SnapshotTicket,
    permit: &crate::apply::LargeObjectPermit,
    page: Vec<LargeObject>,
    io: &IoControl,
) -> Result<Vec<(LargeObjectDone, crate::apply::CopyCommitOutcome)>> {
    use std::sync::{Mutex, atomic::Ordering};
    if page.len() > INVENTORY_PAGE
        || options.large_objects_jobs == 0
        || options.large_objects_jobs > 128
    {
        return Err(refusal("large-objects: invalid bounded worker workset"));
    }
    let jobs = options.large_objects_jobs.min(page.len());
    let queue = Mutex::new(page.into_iter());
    let failure = Mutex::new(None);
    let receipts = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(jobs);
        for _ in 0..jobs {
            let queue = &queue;
            let failure = &failure;
            handles.push(scope.spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut source = PgConnection::connect(&options.source, io)?;
                    crate::snapshot::import(&mut source, ticket, io)?;
                    let mut receipts = Vec::new();
                    loop {
                        crate::copy::check_control(io)?;
                        let object = queue
                            .lock()
                            .map_err(|_| refusal("large-objects: worker queue is unavailable"))?
                            .next();
                        let Some(object) = object else { break };
                        let mut target = PgConnection::connect(&options.target, io)?;
                        crate::catalog::configure_session(&mut target, io)?;
                        let mut transaction =
                            LargeObjectTxn::begin(target, permit, object.oid, io)?;
                        let measurement = copy_object(
                            &mut source,
                            &mut transaction,
                            &object,
                            &options.policy,
                            io,
                        )?;
                        receipts.push(transaction.mark_and_commit(measurement, io)?);
                    }
                    crate::catalog::command(&mut source, "COMMIT", "COMMIT", io)?;
                    Ok::<_, Error>(receipts)
                }));
                match result {
                    Ok(Ok(receipts)) => receipts,
                    result => {
                        let error = match result {
                            Ok(Err(error)) => error,
                            _ => refusal("large-objects: worker terminated without a result"),
                        };
                        let mut first = failure
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        if first.is_none() {
                            *first = Some(error);
                        }
                        io.cancelled.store(true, Ordering::Release);
                        Vec::new()
                    }
                }
            }));
        }
        let mut receipts = Vec::with_capacity(INVENTORY_PAGE);
        for handle in handles {
            match handle.join() {
                Ok(worker) => receipts.extend(worker),
                Err(_) => {
                    let mut first = failure
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if first.is_none() {
                        *first = Some(refusal("large-objects: worker terminated without a result"));
                    }
                    io.cancelled.store(true, Ordering::Release);
                }
            }
        }
        receipts
    });
    if let Some(error) = failure
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        return Err(error);
    }
    crate::copy::check_control(io)?;
    Ok(receipts)
}

fn add_completion(
    completion: &mut crate::contracts::LargeObjectCompletion,
    receipt: &LargeObjectDone,
) -> Result<()> {
    completion.objects = completion
        .objects
        .checked_add(1)
        .ok_or_else(|| refusal("large-objects: object count overflow"))?;
    completion.bytes = completion
        .bytes
        .checked_add(receipt.logical_bytes)
        .ok_or_else(|| refusal("large-objects: byte count overflow"))?;
    Ok(())
}

pub(crate) fn run_initial(
    options: &crate::contracts::CloneOptions,
    plan: &crate::contracts::ClonePlan,
    snapshot: &mut crate::snapshot::CloneSnapshot,
    authority: &mut crate::apply::CloneAuthority,
    io: &IoControl,
) -> Result<crate::contracts::LargeObjectCompletion> {
    let mut completion = crate::contracts::LargeObjectCompletion {
        objects: 0,
        bytes: 0,
    };
    if options.copy_policy() != plan.copy_policy
        || options.policy != plan.policy
        || snapshot.ticket().generation != plan.generation
        || snapshot.ticket().source != plan.source
    {
        return Err(refusal(
            "large-objects: copy policy and snapshot must match the bound plan",
        ));
    }
    if plan.copy_policy.skip_large_objects {
        return Ok(completion);
    }
    if let Some(completion) =
        authority.large_object_completion(crate::apply::LargeObjectPhase::Initial)
    {
        compare_snapshot(options, snapshot, &plan.policy, io)?;
        authority.finish_large_objects(completion, io)?;
        return Ok(completion);
    }
    let ticket = snapshot.ticket().clone();
    let mut after = 0;
    loop {
        let page = inspect_page(snapshot.connection()?, after, &plan.policy, io)?;
        if page.is_empty() {
            break;
        }
        after = page
            .last()
            .ok_or_else(|| refusal("large-objects: inventory cursor is missing"))?
            .oid;
        let count = page.len();
        let permit = authority.large_object_permit(io)?;
        let receipts = run_page(options, &ticket, &permit, page, io)?;
        if receipts.len() != count {
            return Err(refusal(
                "large-objects: object completion result is missing",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for (receipt, outcome) in receipts {
            if !seen.insert(receipt.object_oid) {
                return Err(refusal("large-objects: duplicate object completion"));
            }
            if matches!(outcome, crate::apply::CopyCommitOutcome::Unknown) {
                let durable=authority.resolve_large_object_unknown(receipt.object_oid,io)?.ok_or_else(|| refusal("commit-outcome-unknown: large object receipt is absent; restart the copy generation"))?;
                if durable != receipt {
                    return Err(refusal(
                        "large-objects: durable receipt disagrees with completed object",
                    ));
                }
            }
            add_completion(&mut completion, &receipt)?;
        }
    }
    compare_snapshot(options, snapshot, &plan.policy, io)?;
    authority.finish_large_objects(completion, io)?;
    Ok(completion)
}

pub(crate) fn run_final(
    options: &crate::contracts::CloneOptions,
    plan: &crate::contracts::ClonePlan,
    request: &crate::state::SealedEndpos,
    authority: &mut crate::apply::checkpoint::TargetAuthority,
    io: &IoControl,
) -> Result<crate::contracts::LargeObjectCompletion> {
    let mut completion = crate::contracts::LargeObjectCompletion {
        objects: 0,
        bytes: 0,
    };
    if options.copy_policy() != plan.copy_policy || options.policy != plan.policy {
        return Err(refusal("large-objects: selected copy policy changed"));
    }
    if plan.copy_policy.skip_large_objects {
        return Ok(completion);
    }
    let mut snapshot = crate::snapshot::CloneSnapshot::export(
        PgConnection::connect(&options.source, io)?,
        plan.generation,
        io,
    )?;
    let ticket = snapshot.ticket().clone();
    if ticket.source != plan.source {
        return Err(refusal("large-objects: final source identity changed"));
    }
    if let Some(completion) =
        authority.large_object_completion(crate::apply::LargeObjectPhase::Final)
    {
        compare_snapshot(options, &mut snapshot, &plan.policy, io)?;
        authority.finish_large_objects(plan, request, completion, io)?;
        return Ok(completion);
    }
    let mut after = 0;
    loop {
        let page = inspect_page(snapshot.connection()?, after, &plan.policy, io)?;
        if page.is_empty() {
            break;
        }
        after = page
            .last()
            .ok_or_else(|| refusal("large-objects: inventory cursor is missing"))?
            .oid;
        let count = page.len();
        let permit = authority.large_object_permit(plan, request, io)?;
        let receipts = run_page(options, &ticket, &permit, page, io)?;
        if receipts.len() != count {
            return Err(refusal(
                "large-objects: object completion result is missing",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for (receipt, outcome) in receipts {
            if !seen.insert(receipt.object_oid) {
                return Err(refusal("large-objects: duplicate object completion"));
            }
            if matches!(outcome, crate::apply::CopyCommitOutcome::Unknown) {
                let durable=authority.resolve_large_object_unknown(plan,request,receipt.object_oid,io)?.ok_or_else(|| refusal("commit-outcome-unknown: large object receipt is absent; restart the copy generation"))?;
                if durable != receipt {
                    return Err(refusal(
                        "large-objects: durable receipt disagrees with completed object",
                    ));
                }
            }
            add_completion(&mut completion, &receipt)?;
        }
    }
    authority.reconcile_large_objects(snapshot.connection()?, plan, request, io)?;
    compare_snapshot(options, &mut snapshot, &plan.policy, io)?;
    authority.finish_large_objects(plan, request, completion, io)?;
    Ok(completion)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectiveAcl {
    owner: String,
    grants: Vec<Grant>,
}

fn effective_acl(connection: &mut PgConnection, oid: u32, io: &IoControl) -> Result<EffectiveAcl> {
    let sql = format!(
        "SELECT CASE WHEN pg_catalog.octet_length(value)<=65536 THEN value END FROM (SELECT pg_catalog.json_build_object('owner',pg_catalog.pg_get_userbyid(m.lomowner),'grants',(SELECT COALESCE(pg_catalog.json_agg(pg_catalog.json_build_object('grantor',pg_catalog.pg_get_userbyid(a.grantor),'grantee',CASE WHEN a.grantee=0 THEN NULL ELSE pg_catalog.pg_get_userbyid(a.grantee) END,'privilege',a.privilege_type,'grantable',a.is_grantable)),'[]'::pg_catalog.json) FROM pg_catalog.aclexplode(COALESCE(m.lomacl,pg_catalog.acldefault('L',m.lomowner))) AS a))::pg_catalog.text AS value FROM pg_catalog.pg_largeobject_metadata AS m WHERE m.oid={oid}::pg_catalog.oid) AS bounded"
    );
    let result = connection.query(&sql, io)?;
    if result.rows() != 1 || result.columns()? != 1 {
        return Err(refusal("large-objects: effective ACL object is absent"));
    }
    let bytes = text(&result, 0, 0)?.as_bytes();
    if bytes.len() > METADATA_BYTES {
        return Err(refusal("large-objects: effective ACL exceeds its budget"));
    }
    let mut acl: EffectiveAcl = serde_json::from_slice(bytes)
        .map_err(|_| refusal("large-objects: invalid effective ACL"))?;
    if acl.owner.is_empty()
        || acl.grants.iter().any(|grant| {
            grant.grantor.is_empty()
                || grant.grantee.as_ref().is_some_and(|role| role.is_empty())
                || !matches!(grant.privilege.as_str(), "SELECT" | "UPDATE")
        })
    {
        return Err(refusal(
            "large-objects: invalid effective ACL role or privilege",
        ));
    }
    acl.grants.sort();
    Ok(acl)
}

fn expected_acl(delta: Option<&AclDelta>, owner: &str) -> Vec<Grant> {
    let mut expected = vec![
        Grant {
            grantor: owner.to_owned(),
            grantee: Some(owner.to_owned()),
            privilege: "SELECT".into(),
            grantable: false,
        },
        Grant {
            grantor: owner.to_owned(),
            grantee: Some(owner.to_owned()),
            privilege: "UPDATE".into(),
            grantable: false,
        },
    ];
    if let Some(delta) = delta {
        if delta.revoke_owner_default {
            expected.retain(|grant| grant.grantee.as_deref() != Some(delta.source_owner.as_str()));
        }
        for grant in &delta.grants {
            let mut grant = grant.clone();
            if grant.grantor == delta.source_owner {
                grant.grantor = owner.to_owned();
            }
            if let Some(existing) = expected.iter_mut().find(|existing| {
                existing.grantor == grant.grantor
                    && existing.grantee == grant.grantee
                    && existing.privilege == grant.privilege
            }) {
                existing.grantable |= grant.grantable;
            } else {
                expected.push(grant);
            }
        }
    }
    expected.sort();
    expected
}

fn verify_selected_metadata(
    connection: &mut PgConnection,
    oid: u32,
    source: &str,
    policy: &RestorePolicy,
    io: &IoControl,
) -> Result<ActualMetadata> {
    let source: SelectedMetadata =
        serde_json::from_str(&canonical_metadata(source.as_bytes(), policy)?)
            .map_err(|_| refusal("large-objects: selected metadata cannot be decoded"))?;
    let (target, actual) = inspect_exact(connection, oid, policy, io)?;
    let target: SelectedMetadata = serde_json::from_str(&target)
        .map_err(|_| refusal("large-objects: selected metadata cannot be decoded"))?;
    if source.owner != target.owner || source.comment != target.comment {
        return Err(refusal("large-objects: selected owner or comment differs"));
    }
    if !policy.no_acl {
        let acl = effective_acl(connection, oid, io)?;
        if expected_acl(source.acl.as_ref(), &acl.owner) != acl.grants {
            return Err(refusal(
                "large-objects: selected ACL differs from restore policy result",
            ));
        }
    }
    Ok(actual)
}

fn apply_metadata(
    connection: &mut PgConnection,
    oid: u32,
    source: &str,
    policy: &RestorePolicy,
    io: &IoControl,
) -> Result<()> {
    let selected: SelectedMetadata =
        serde_json::from_str(&canonical_metadata(source.as_bytes(), policy)?)
            .map_err(|_| refusal("large-objects: selected metadata cannot be decoded"))?;
    if let Some(owner) = &selected.owner {
        let owner = connection.quote_identifier(owner)?;
        crate::catalog::command(
            connection,
            &format!("ALTER LARGE OBJECT {oid} OWNER TO {owner}"),
            "ALTER LARGE OBJECT",
            io,
        )?;
    }
    if let Some(acl) = &selected.acl {
        let identity = connection.query("SELECT SESSION_USER,CURRENT_USER", io)?;
        if identity.rows() != 1 || identity.columns()? != 2 {
            return Err(refusal("large-objects: session identity is unavailable"));
        }
        let session = connection.quote_identifier(text(&identity, 0, 0)?)?;
        let role = connection.quote_identifier(text(&identity, 0, 1)?)?;
        let actual_owner = effective_acl(connection, oid, io)?.owner;
        let target_owner = connection.quote_identifier(&actual_owner)?;
        if acl.revoke_owner_default {
            let owner = connection.quote_identifier(&acl.source_owner)?;
            crate::catalog::command(
                connection,
                &format!("REVOKE ALL ON LARGE OBJECT {oid} FROM {owner}"),
                "REVOKE",
                io,
            )?;
        }
        for grant in &acl.grants {
            if grant.grantor == acl.source_owner {
                crate::catalog::command(
                    connection,
                    &format!("SET LOCAL ROLE {target_owner}"),
                    "SET",
                    io,
                )?;
            } else {
                let grantor = connection.quote_identifier(&grant.grantor)?;
                crate::catalog::command(
                    connection,
                    &format!("SET LOCAL SESSION AUTHORIZATION {grantor}"),
                    "SET",
                    io,
                )?;
            }
            let grantee = match &grant.grantee {
                Some(role) => connection.quote_identifier(role)?,
                None => "PUBLIC".into(),
            };
            let option = if grant.grantable {
                " WITH GRANT OPTION"
            } else {
                ""
            };
            crate::catalog::command(
                connection,
                &format!(
                    "GRANT {} ON LARGE OBJECT {oid} TO {grantee}{option}",
                    grant.privilege
                ),
                "GRANT",
                io,
            )?;
            crate::catalog::command(
                connection,
                &format!("SET LOCAL SESSION AUTHORIZATION {session}"),
                "SET",
                io,
            )?;
            crate::catalog::command(connection, &format!("SET LOCAL ROLE {role}"), "SET", io)?;
        }
    }
    if !policy.no_comments {
        let comment = match &selected.comment {
            Some(comment) => connection.quote_literal(comment)?,
            None => "NULL".into(),
        };
        crate::catalog::command(
            connection,
            &format!("COMMENT ON LARGE OBJECT {oid} IS {comment}"),
            "COMMENT",
            io,
        )?;
    }
    Ok(())
}

pub(crate) fn copy_object(
    source: &mut PgConnection,
    target: &mut LargeObjectTxn,
    object: &LargeObject,
    policy: &RestorePolicy,
    io: &IoControl,
) -> Result<LargeObjectMeasurement> {
    require_transaction(source)?;
    if object.oid == 0 || object.oid != target.object_oid() {
        return Err(refusal(
            "large-objects: object does not match permitted transaction",
        ));
    }
    let (metadata, _) = inspect_exact(source, object.oid, policy, io)?;
    if metadata != object.source_metadata {
        return Err(refusal(
            "large-objects: source metadata changed within the snapshot",
        ));
    }
    if target.existing_receipt().is_some()
        && scalar(
            target.connection_mut()?,
            &format!(
                "SELECT pg_catalog.lo_unlink({}::pg_catalog.oid)",
                object.oid
            ),
            io,
        )? != 1
    {
        return Err(refusal("large-objects: owned object did not unlink"));
    }
    let connection = target.connection_mut()?;
    require_transaction(connection)?;
    if scalar(
        connection,
        &format!(
            "SELECT pg_catalog.lo_create({}::pg_catalog.oid)",
            object.oid
        ),
        io,
    )? != u64::from(object.oid)
    {
        return Err(refusal("large-objects: created object OID differs"));
    }
    let source_descriptor = open(source, object.oid, INV_READ, io)?;
    let target_descriptor = open(connection, object.oid, INV_WRITE, io)?;
    connection.prepare("rpgmigrate_lo_write","SELECT pg_catalog.lowrite($1::pg_catalog.int4,pg_catalog.decode($2::pg_catalog.text,'hex'))",&[23,25],io)?;
    let descriptor = target_descriptor.to_string();
    let mut bytes = 0u64;
    let mut digest = Sha256::new();
    loop {
        let chunk = read_chunk(source, source_descriptor, io)?;
        if chunk.is_empty() {
            break;
        }
        let encoded = encode_hex(&chunk)?;
        let result = connection.execute_prepared(
            "rpgmigrate_lo_write",
            &[
                PgParam::Text(descriptor.as_bytes()),
                PgParam::Text(encoded.as_bytes()),
            ],
            io,
        )?;
        if result.rows() != 1
            || result.columns()? != 1
            || text(&result, 0, 0)?.parse::<usize>().ok() != Some(chunk.len())
        {
            return Err(refusal(
                "large-objects: bounded write did not write the complete chunk",
            ));
        }
        bytes = bytes
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| refusal("large-objects: logical byte count overflow"))?;
        digest.update(&chunk);
    }
    close(source, source_descriptor, io)?;
    close(connection, target_descriptor, io)?;
    apply_metadata(connection, object.oid, &metadata, policy, io)?;
    let actual = verify_selected_metadata(connection, object.oid, &metadata, policy, io)?;
    let actual_bytes = serde_json::to_vec(&actual)
        .map_err(|_| refusal("large-objects: actual metadata cannot be encoded"))?;
    if metadata
        .len()
        .checked_add(actual_bytes.len())
        .is_none_or(|bytes| bytes > METADATA_BYTES)
    {
        return Err(refusal(
            "large-objects: object metadata exceeds its bounded budget",
        ));
    }
    let hash: [u8; 32] = digest.finalize().into();
    if fingerprint(connection, object.oid, io)? != (bytes, hash) {
        return Err(refusal(
            "large-objects: copied object logical content differs",
        ));
    }
    Ok(LargeObjectMeasurement {
        target_owner: actual.owner,
        target_acl: actual.acl,
        target_comment: actual.comment,
        source_metadata: metadata,
        logical_bytes: bytes,
        content_hash: hash,
    })
}

fn compare_snapshot(
    options: &crate::contracts::CloneOptions,
    snapshot: &mut crate::snapshot::CloneSnapshot,
    policy: &RestorePolicy,
    io: &IoControl,
) -> Result<()> {
    let mut target = PgConnection::connect(&options.target, io)?;
    crate::catalog::configure_session(&mut target, io)?;
    crate::catalog::command(
        &mut target,
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "BEGIN",
        io,
    )?;
    compare(snapshot.connection()?, &mut target, policy, io)?;
    crate::catalog::command(&mut target, "COMMIT", "COMMIT", io)
}
