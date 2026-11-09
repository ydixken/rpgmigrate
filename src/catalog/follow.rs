use super::*;
use crate::contracts::{
    ClonePlan, CloneTable, CopyGeneration, FollowBinding, OwnedCopyTable, PublicationReceipt,
    RunId, SourceType,
};
use crate::identity::SourceEndLsn;

const FOLLOW_CATALOG_BYTES: usize = 1024 * 1024;
const FOLLOW_MAX_TABLES: usize = 4096;

#[derive(Debug)]
pub(crate) struct FollowRelation {
    pub(crate) relation: Event,
    pub(crate) target_type_oids: Vec<u32>,
    pub(crate) identity_modes: Vec<u8>,
}

fn retention_policy(
    major: u32,
    wal_level: &str,
    keep: &str,
    old: Option<&str>,
    idle: Option<&str>,
) -> Result<()> {
    if !matches!(major, 16 | 18)
        || wal_level != "logical"
        || keep != "-1"
        || (major == 16 && old != Some("-1"))
        || (major == 18 && idle != Some("0"))
    {
        return Err(refusal(
            "follow-retention: require logical WAL, unlimited slot retention, and disabled snapshot/idle-slot expiry",
        ));
    }
    Ok(())
}

pub(crate) fn reject_generated(tables: &[CloneTable]) -> Result<()> {
    if tables
        .iter()
        .any(|table| table.all_columns != table.writable_columns)
    {
        return Err(refusal(
            "follow-preflight: generated columns are unsupported",
        ));
    }
    Ok(())
}

pub(crate) fn resource_name(kind: &str, run: RunId, generation: CopyGeneration) -> Result<String> {
    if run.0 == [0; 16] || generation.0 == 0 || !matches!(kind, "slot" | "publication") {
        return Err(refusal("follow-source: invalid native resource identity"));
    }
    let run = run
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let prefix = if kind == "slot" { "rpgm_s" } else { "rpgm_p" };
    Ok(format!("{prefix}_{run}_{:x}", generation.0))
}

fn slot_lsn(value: &str) -> Result<SourceEndLsn> {
    let invalid = || refusal("follow-gap: slot returned an invalid WAL position");
    let (high, low) = value.split_once('/').ok_or_else(invalid)?;
    let parse = |part: &str| {
        if part.is_empty() || part.len() > 8 || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid());
        }
        u32::from_str_radix(part, 16).map_err(|_| invalid())
    };
    let value = (u64::from(parse(high)?) << 32) | u64::from(parse(low)?);
    if value == 0 {
        return Err(invalid());
    }
    Ok(SourceEndLsn(value))
}

pub(crate) fn preflight_follow(
    source: &mut PgConnection,
    plan: &ClonePlan,
    io: &IoControl,
) -> Result<()> {
    preflight_follow_restart(source, plan, io)?;
    let capacity = source.query("SELECT (r.rolsuper OR r.rolreplication),pg_catalog.has_database_privilege(pg_catalog.current_database(),'CREATE'),(SELECT count(*) FROM pg_catalog.pg_replication_slots)<pg_catalog.current_setting('max_replication_slots')::pg_catalog.int4,(SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE backend_type='walsender')<pg_catalog.current_setting('max_wal_senders')::pg_catalog.int4 FROM pg_catalog.pg_roles r WHERE r.rolname=CURRENT_USER",io)?;
    if capacity.rows() != 1
        || capacity.columns()? != 4
        || (0..4).any(|column| capacity.cell(0, column).ok() != Some(Some(b"t")))
    {
        return Err(refusal(
            "follow-preflight: replication privilege, publication privilege, or source capacity is insufficient",
        ));
    }
    Ok(())
}

pub(crate) fn preflight_follow_restart(
    source: &mut PgConnection,
    plan: &ClonePlan,
    io: &IoControl,
) -> Result<()> {
    reject_generated(&plan.tables)?;
    if plan.tables.len() > FOLLOW_MAX_TABLES || read_clone_source(source, io)? != plan.source {
        return Err(refusal(
            "follow-preflight: source identity or table bound differs from the plan",
        ));
    }
    require_setting(source, "server_encoding", "UTF8", io)?;
    require_durable_storage(source, io)?;
    check_retention(source, plan.source.major, io)?;
    inspect_source_tables(source, plan, io)
}

fn check_retention(source: &mut PgConnection, major: u32, io: &IoControl) -> Result<()> {
    let settings = source.query("SELECT pg_catalog.current_setting('wal_level'),pg_catalog.current_setting('max_slot_wal_keep_size'),pg_catalog.current_setting('old_snapshot_threshold',true),pg_catalog.current_setting('idle_replication_slot_timeout',true)",io)?;
    if settings.rows() != 1 || settings.columns()? != 4 {
        return Err(refusal("follow-retention: settings evidence is missing"));
    }
    retention_policy(
        major,
        text(&settings, 0, 0)?,
        text(&settings, 0, 1)?,
        settings
            .cell(0, 2)?
            .map(|s| std::str::from_utf8(s))
            .transpose()
            .map_err(|_| refusal("follow-retention: invalid snapshot policy"))?,
        settings
            .cell(0, 3)?
            .map(|s| std::str::from_utf8(s))
            .transpose()
            .map_err(|_| refusal("follow-retention: invalid idle-slot policy"))?,
    )
}

fn inspect_source_tables(
    source: &mut PgConnection,
    plan: &ClonePlan,
    io: &IoControl,
) -> Result<()> {
    super::clone::require_identity_compatibility(
        plan.source.major,
        plan.target.major,
        &plan.tables,
    )?;
    for table in &plan.tables {
        let oid = table.source_oid;
        let evidence=source.query(&format!("SELECT c.oid,n.nspname,c.relname,pg_catalog.pg_has_role(c.relowner,'USAGE'),c.relreplident,NOT c.relrowsecurity AND NOT c.relforcerowsecurity,NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated<>''),NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid AND t.tgenabled IN ('A','R')) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=c.oid AND r.ev_enabled IN ('A','R')) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid={oid} AND c.relkind='r' AND c.relpersistence='p'"),io)?;
        if evidence.rows() != 1
            || evidence.columns()? != 8
            || text(&evidence, 0, 0)? != oid.to_string()
            || text(&evidence, 0, 1)? != table.name.schema
            || text(&evidence, 0, 2)? != table.name.table
            || [3, 5, 6, 7]
                .iter()
                .any(|column| evidence.cell(0, *column).ok() != Some(Some(b"t")))
        {
            return Err(refusal(
                "follow-preflight: source heap identity, ownership, generated column, RLS, or trigger policy is unsupported",
            ));
        }
        let (local, root) =
            super::clone::clone_identity_modes(source, oid, &table.all_columns, io)?;
        if local != table.identity_modes || root != table.partition_identity_modes {
            return Err(refusal(
                "follow-preflight: source identity generation modes differ from the plan",
            ));
        }
        let identity = text(&evidence, 0, 4)?;
        if !matches!(identity, "d" | "i" | "f") {
            return Err(refusal(
                "follow-preflight: source table lacks supported replica identity",
            ));
        }
        if identity != "f" {
            identity_columns(source, oid, identity, io)?;
        }
        require_follow_execution(source, oid, io)?;
    }
    Ok(())
}

fn require_follow_execution(connection: &mut PgConnection, oid: u32, io: &IoControl) -> Result<()> {
    // Pinned builtins can lack pg_depend rows, so expression absence is the safe admission proof.
    let result=connection.query(&format!(r#"WITH RECURSIVE ancestors(oid) AS (
 SELECT {oid}::pg_catalog.oid UNION SELECT i.inhparent FROM pg_catalog.pg_inherits i JOIN ancestors a ON a.oid=i.inhrelid
), types(oid,depth) AS (
 SELECT DISTINCT a.atttypid,0 FROM pg_catalog.pg_attribute a WHERE a.attrelid={oid} AND a.attnum>0 AND NOT a.attisdropped
 UNION ALL SELECT CASE WHEN t.typtype='d' THEN t.typbasetype ELSE t.typelem END,x.depth+1 FROM types x JOIN pg_catalog.pg_type t ON t.oid=x.oid WHERE x.depth<32 AND (t.typtype='d' OR (t.typelem<>0 AND t.typlen=-1))
)
SELECT
 NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.contype IN ('c','x') AND (c.conrelid IN (SELECT oid FROM ancestors) OR c.contypid IN (SELECT oid FROM types))),
 NOT EXISTS(SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid JOIN pg_catalog.pg_am am ON am.oid=idx.relam WHERE i.indrelid IN (SELECT oid FROM ancestors) AND (
 i.indexprs IS NOT NULL OR i.indpred IS NOT NULL OR NOT i.indisvalid OR NOT i.indisready OR NOT i.indislive OR am.oid>=16384 OR am.amname NOT IN ('btree','hash','gist','gin','spgist','brin') OR
 EXISTS(SELECT 1 FROM pg_catalog.unnest(i.indclass::pg_catalog.oid[]) key(oid) LEFT JOIN pg_catalog.pg_opclass op ON op.oid=key.oid LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid=op.opcnamespace WHERE op.oid IS NULL OR op.oid>=16384 OR ns.nspname<>'pg_catalog' OR op.opcmethod<>am.oid) OR
 EXISTS(SELECT 1 FROM pg_catalog.unnest(i.indcollation::pg_catalog.oid[]) key(oid) LEFT JOIN pg_catalog.pg_collation co ON co.oid=key.oid WHERE key.oid<>0 AND (co.oid IS NULL OR NOT co.collisdeterministic OR co.collversion IS DISTINCT FROM pg_catalog.pg_collation_actual_version(co.oid)))
 )),
 NOT EXISTS(SELECT 1 FROM pg_catalog.pg_partitioned_table p WHERE p.partrelid IN (SELECT oid FROM ancestors) AND (
 p.partexprs IS NOT NULL OR 0=ANY(p.partattrs::pg_catalog.int2[]) OR
 EXISTS(SELECT 1 FROM pg_catalog.unnest(p.partclass::pg_catalog.oid[]) key(oid) LEFT JOIN pg_catalog.pg_opclass op ON op.oid=key.oid LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid=op.opcnamespace LEFT JOIN pg_catalog.pg_am am ON am.oid=op.opcmethod WHERE op.oid IS NULL OR op.oid>=16384 OR ns.nspname<>'pg_catalog' OR am.oid>=16384 OR am.amname NOT IN ('btree','hash')) OR
 EXISTS(SELECT 1 FROM pg_catalog.unnest(p.partcollation::pg_catalog.oid[]) key(oid) LEFT JOIN pg_catalog.pg_collation co ON co.oid=key.oid LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid=co.collnamespace WHERE key.oid<>0 AND (co.oid IS NULL OR ns.nspname<>'pg_catalog' OR co.collname NOT IN ('C','POSIX') OR NOT co.collisdeterministic OR co.collversion IS DISTINCT FROM pg_catalog.pg_collation_actual_version(co.oid)))
 ))"#),io)?;
    if result.rows() != 1 || result.columns()? != 3 {
        return Err(refusal(
            "follow-execution: catalog safety evidence is missing",
        ));
    }
    for (column, message) in [
        (
            0,
            "follow-execution: table/domain CHECK and exclusion constraints are unsupported",
        ),
        (
            1,
            "follow-execution: require plain indexes with built-in methods/opclasses and deterministic current collations",
        ),
        (
            2,
            "follow-execution: partition keys require plain columns, built-in opclasses, and noncollatable or C/POSIX keys",
        ),
    ] {
        if result.cell(0, column)? != Some(b"t") {
            return Err(refusal(message));
        }
    }
    Ok(())
}

fn exact_membership(
    expected: &[CloneTable],
    actual: &[(u32, QualifiedTable, Vec<String>)],
) -> Result<()> {
    if expected.len() != actual.len() || expected.len() > FOLLOW_MAX_TABLES {
        return Err(refusal(
            "follow-gap: publication physical membership differs from the admitted plan",
        ));
    }
    let mut seen = BTreeSet::new();
    for (oid, name, columns) in actual {
        if !seen.insert(*oid)
            || expected
                .iter()
                .filter(|table| {
                    table.source_oid == *oid && table.name == *name && table.all_columns == *columns
                })
                .count()
                != 1
        {
            return Err(refusal(
                "follow-gap: publication physical membership differs from the admitted plan",
            ));
        }
    }
    Ok(())
}

pub(crate) fn publication_membership(
    source: &mut PgConnection,
    receipt: &PublicationReceipt,
    tables: &[CloneTable],
    io: &IoControl,
) -> Result<()> {
    let name = source.quote_literal(&receipt.name)?;
    let members=source.query(&format!("SELECT c.oid,p.schemaname,p.tablename,pg_catalog.to_json(p.attnames)::pg_catalog.text,p.rowfilter IS NULL AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated<>'') FROM pg_catalog.pg_publication_tables p JOIN pg_catalog.pg_namespace n ON n.nspname=p.schemaname JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=p.tablename WHERE p.pubname={name} ORDER BY c.oid LIMIT 4097"),io)?;
    if members.columns()? != 5 || members.rows() > FOLLOW_MAX_TABLES {
        return Err(refusal(
            "follow-gap: publication membership evidence is invalid",
        ));
    }
    let mut actual = Vec::with_capacity(members.rows());
    for row in 0..members.rows() {
        if members.cell(row, 4)? != Some(b"t") {
            return Err(refusal(
                "follow-gap: publication row filters are unsupported",
            ));
        }
        let columns: Vec<String> = serde_json::from_str(text(&members, row, 3)?)
            .map_err(|_| refusal("follow-gap: publication column evidence is invalid"))?;
        actual.push((
            cdc_object_id(text(&members, row, 0)?)?,
            QualifiedTable {
                schema: text(&members, row, 1)?.into(),
                table: text(&members, row, 2)?.into(),
            },
            columns,
        ));
    }
    exact_membership(tables, &actual)
}

fn identity_columns(
    connection: &mut PgConnection,
    oid: u32,
    identity: &str,
    io: &IoControl,
) -> Result<BTreeSet<usize>> {
    if identity == "f" {
        return Ok(BTreeSet::new());
    }
    let index=connection.query(&format!("SELECT i.indkey::pg_catalog.text,i.indnkeyatts,i.indnatts,i.indisunique AND i.indisvalid AND i.indisready AND i.indislive AND i.indimmediate AND NOT i.indcheckxmin AND i.indexprs IS NULL AND i.indpred IS NULL AND am.amname='btree' AND am.oid<16384 AND NOT EXISTS(SELECT 1 FROM pg_catalog.unnest(i.indclass::pg_catalog.oid[]) k(oid) JOIN pg_catalog.pg_opclass o ON o.oid=k.oid WHERE o.oid>=16384) FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid JOIN pg_catalog.pg_am am ON am.oid=c.relam WHERE i.indrelid={oid} AND {} LIMIT 2",if identity=="i" {"i.indisreplident"} else {"i.indisprimary"}),io)?;
    if index.rows() != 1 || index.columns()? != 4 || index.cell(0, 3)? != Some(b"t") {
        return Err(refusal(
            "follow-preflight: replica identity requires one valid immediate built-in unique index",
        ));
    }
    let nkeys: usize = text(&index, 0, 1)?
        .parse()
        .map_err(|_| refusal("follow-preflight: invalid identity key count"))?;
    let nattrs: usize = text(&index, 0, 2)?
        .parse()
        .map_err(|_| refusal("follow-preflight: invalid identity attribute count"))?;
    let numbers = text(&index, 0, 0)?
        .split_whitespace()
        .map(cdc_attribute_number)
        .collect::<Result<Vec<_>>>()?;
    if nkeys == 0 || nkeys > nattrs || nattrs != numbers.len() || nattrs > 1600 {
        return Err(refusal("follow-preflight: invalid identity index layout"));
    }
    let keys = numbers[..nkeys].iter().copied().collect::<BTreeSet<_>>();
    if keys.len() != nkeys {
        return Err(refusal("follow-preflight: duplicate identity key column"));
    }
    Ok(keys)
}

pub(crate) fn publication_receipt(
    source: &mut PgConnection,
    name: &str,
    io: &IoControl,
) -> Result<PublicationReceipt> {
    if name.is_empty() || name.len() > 63 || name.contains('\0') {
        return Err(refusal("follow-source: invalid publication name"));
    }
    let quoted = source.quote_literal(name)?;
    let result=source.query(&format!("SELECT p.oid,p.pubowner,NOT p.puballtables AND p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate AND NOT p.pubviaroot AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_publication_namespace n WHERE n.pnpubid=p.oid),COALESCE(pg_catalog.to_jsonb(p)->>'pubgencols','n') FROM pg_catalog.pg_publication p WHERE p.pubname={quoted}"),io)?;
    if result.rows() != 1
        || result.columns()? != 4
        || result.cell(0, 2)? != Some(b"t")
        || text(&result, 0, 3)? != "n"
    {
        return Err(refusal(
            "follow-gap: publication identity or options are missing or unsupported",
        ));
    }
    Ok(PublicationReceipt {
        name: name.into(),
        oid: cdc_object_id(text(&result, 0, 0)?)?,
        owner_oid: cdc_object_id(text(&result, 0, 1)?)?,
    })
}

pub(crate) fn source_types(
    source: &mut PgConnection,
    tables: &[CloneTable],
    io: &IoControl,
) -> Result<Vec<SourceType>> {
    if tables.is_empty() {
        return Ok(Vec::new());
    }
    let oids = tables
        .iter()
        .map(|table| table.source_oid.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let count=source.query(&format!("SELECT count(DISTINCT atttypid) FROM pg_catalog.pg_attribute WHERE attrelid IN ({oids}) AND attnum>0 AND NOT attisdropped AND atttypid>=16384"),io)?;
    if count.rows() != 1 || count.columns()? != 1 {
        return Err(refusal("follow-source: source type inventory is missing"));
    }
    let expected = text(&count, 0, 0)?
        .parse::<usize>()
        .map_err(|_| refusal("follow-source: invalid source type inventory"))?;
    if expected > 16_384 {
        return Err(refusal("follow-source: type map exceeds its bound"));
    }
    // pgoutput Y retains the remote domain OID but writes getBaseType's qualified name.
    let result=source.query(&format!("WITH RECURSIVE types AS (SELECT DISTINCT a.atttypid AS remote,a.atttypid AS base,0 AS depth FROM pg_catalog.pg_attribute a WHERE a.attrelid IN ({oids}) AND a.attnum>0 AND NOT a.attisdropped AND a.atttypid>=16384 UNION ALL SELECT x.remote,t.typbasetype,x.depth+1 FROM types x JOIN pg_catalog.pg_type t ON t.oid=x.base WHERE t.typtype='d' AND x.depth<32) SELECT x.remote,n.nspname,t.typname FROM types x JOIN pg_catalog.pg_type t ON t.oid=x.base JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE t.typtype<>'d' ORDER BY x.remote LIMIT 16385"),io)?;
    if result.columns()? != 3 || result.rows() != expected {
        return Err(refusal("follow-source: type map exceeds its bound"));
    }
    let mut remaining = FOLLOW_CATALOG_BYTES;
    let mut types = Vec::new();
    let mut previous = 0;
    for row in 0..result.rows() {
        let oid = cdc_object_id(text(&result, row, 0)?)?;
        if oid < 16384 || oid <= previous {
            return Err(refusal(
                "follow-source: type identities are not unique and ordered",
            ));
        }
        previous = oid;
        let item = SourceType {
            oid,
            schema: text(&result, row, 1)?.into(),
            name: text(&result, row, 2)?.into(),
        };
        require_copy_name(&QualifiedTable {
            schema: item.schema.clone(),
            table: item.name.clone(),
        })?;
        remaining = remaining
            .checked_sub(std::mem::size_of::<SourceType>() + item.schema.len() + item.name.len())
            .ok_or_else(|| refusal("follow-source: type map exceeds one MiB"))?;
        types.push(item);
    }
    Ok(types)
}

pub(crate) fn check_follow_health(
    source: &mut PgConnection,
    plan: &ClonePlan,
    binding: &FollowBinding,
    expected_active_pid: Option<i32>,
    io: &IoControl,
) -> Result<()> {
    follow_health(source, plan, binding, expected_active_pid, io).map(|_| ())
}

pub(crate) fn follow_confirmed_flush(
    source: &mut PgConnection,
    plan: &ClonePlan,
    binding: &FollowBinding,
    expected_active_pid: Option<i32>,
    io: &IoControl,
) -> Result<SourceEndLsn> {
    follow_health(source, plan, binding, expected_active_pid, io)
}

pub(crate) fn follow_start_confirmed_flush(
    source: &mut PgConnection,
    binding: &FollowBinding,
    receiver_pid: i32,
    io: &IoControl,
) -> Result<SourceEndLsn> {
    if receiver_pid <= 0 {
        return Err(refusal(
            "follow-gap: receiver requires a positive active slot owner",
        ));
    }
    let io = &follow_health_control(io);
    require_follow_source(source, binding, io)?;
    let (confirmed, active_pid) = read_follow_slot(source, binding, io)?;
    require_slot_owner(active_pid, Some(receiver_pid))?;
    if active_pid != Some(receiver_pid) {
        return Err(refusal(
            "follow-gap: receiver does not hold the active source slot",
        ));
    }
    Ok(confirmed)
}

fn follow_health_control(io: &IoControl) -> IoControl {
    let mut bounded = crate::pg::operation_control(io);
    bounded.deadline = bounded
        .deadline
        .min(std::time::Instant::now() + std::time::Duration::from_secs(10));
    bounded
}

fn require_follow_source(
    source: &mut PgConnection,
    binding: &FollowBinding,
    io: &IoControl,
) -> Result<crate::contracts::CloneSource> {
    if source.is_replication()? || source.transaction_status()? != TransactionStatus::Idle {
        return Err(refusal(
            "follow-gap: source health requires its idle ordinary SQL session",
        ));
    }
    let actual = read_clone_source(source, io)?;
    if actual.identity != binding.stream().source || actual.timeline != binding.source_timeline {
        return Err(refusal(
            "follow-gap: source identity or timeline differs from the owned slot binding",
        ));
    }
    require_durable_storage(source, io).map_err(|mut error| {
        if error.kind == ErrorKind::BadConfig {
            error.kind = ErrorKind::BadState;
        }
        error
    })?;
    Ok(actual)
}

fn follow_health(
    source: &mut PgConnection,
    plan: &ClonePlan,
    binding: &FollowBinding,
    expected_active_pid: Option<i32>,
    io: &IoControl,
) -> Result<SourceEndLsn> {
    let io = &follow_health_control(io);
    if plan.source.identity != binding.stream().source
        || plan.source.timeline != binding.source_timeline
        || plan.generation != binding.copy_generation
        || require_follow_source(source, binding, io)? != plan.source
    {
        return Err(refusal(
            "follow-gap: source identity, timeline, generation, or health session changed",
        ));
    }
    if publication_receipt(source, &binding.publication.name, io)? != binding.publication {
        return Err(refusal("follow-gap: publication was replaced"));
    }
    publication_membership(source, &binding.publication, &plan.tables, io)?;
    check_retention(source, plan.source.major, io)?;
    if source_types(source, &plan.tables, io)? != binding.source_types {
        return Err(refusal("follow-gap: source type mapping changed"));
    }
    let (confirmed, active_pid) = read_follow_slot(source, binding, io)?;
    require_slot_owner(active_pid, expected_active_pid)?;
    Ok(confirmed)
}

fn read_follow_slot(
    source: &mut PgConnection,
    binding: &FollowBinding,
    io: &IoControl,
) -> Result<(SourceEndLsn, Option<i32>)> {
    if binding.slot.intent.binding.plugin() != crate::cdc::contracts::Plugin::Pgoutput {
        return Err(refusal(
            "follow-gap: owned source slot plugin is unsupported",
        ));
    }
    let slot = source.quote_literal(binding.slot.intent.binding.slot_name())?;
    let result=source.query(&format!("SELECT s.plugin,s.slot_type,s.datoid,s.temporary,s.wal_status,s.restart_lsn,s.confirmed_flush_lsn,COALESCE(s.conflicting,false),COALESCE(pg_catalog.to_jsonb(s)->>'invalidation_reason',''),COALESCE(pg_catalog.to_jsonb(s)->>'failover','false'),COALESCE(pg_catalog.to_jsonb(s)->>'synced','false'),s.active_pid FROM pg_catalog.pg_replication_slots s WHERE s.slot_name={slot}"),io)?;
    if result.rows() != 1
        || result.columns()? != 12
        || text(&result, 0, 0)? != "pgoutput"
        || text(&result, 0, 1)? != "logical"
        || text(&result, 0, 2)? != binding.stream().source.database_oid.to_string()
        || result.cell(0, 3)? != Some(b"f")
        || !matches!(text(&result, 0, 4)?, "reserved" | "extended")
        || result.cell(0, 7)? != Some(b"f")
        || !text(&result, 0, 8)?.is_empty()
        || text(&result, 0, 9)? != "false"
        || text(&result, 0, 10)? != "false"
    {
        return Err(refusal(
            "follow-gap: source slot is missing, foreign, invalidated, or has unsafe WAL retention",
        ));
    }
    let restart = slot_lsn(text(&result, 0, 5)?)?;
    let confirmed = slot_lsn(text(&result, 0, 6)?)?;
    let active_pid = result
        .cell(0, 11)?
        .map(|value| {
            std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<i32>().ok())
                .filter(|pid| *pid > 0)
                .ok_or_else(|| refusal("follow-gap: invalid active slot owner"))
        })
        .transpose()?;
    if confirmed < binding.slot.consistent_point || restart > confirmed {
        return Err(refusal(
            "follow-gap: slot WAL positions conflict with the owned creation receipt",
        ));
    }
    Ok((confirmed, active_pid))
}

fn require_slot_owner(active: Option<i32>, expected: Option<i32>) -> Result<()> {
    if expected.is_some_and(|pid| pid <= 0)
        || active.is_some_and(|pid| pid <= 0 || Some(pid) != expected)
    {
        return Err(refusal(
            "follow-gap: source slot has a competing active owner",
        ));
    }
    Ok(())
}

pub(crate) fn preflight_follow_relation(
    target: &mut PgConnection,
    expected: &Event,
    binding: &FollowBinding,
    owned: &OwnedCopyTable,
    io: &IoControl,
) -> Result<FollowRelation> {
    let Event::Relation {
        key,
        schema,
        name,
        columns,
        replica_identity,
    } = expected
    else {
        return Err(refusal("follow-target: relation metadata is required"));
    };
    if target.is_replication()?
        || target.transaction_status()? != TransactionStatus::InTransaction
        || key.oid != owned.table.source_oid
        || key.generation == 0
        || schema != &owned.table.name.schema
        || name != &owned.table.name.table
        || owned.generation != binding.copy_generation
        || columns.len() != owned.table.all_columns.len()
        || columns
            .iter()
            .zip(&owned.table.all_columns)
            .any(|(column, name)| &column.name != name)
        || !matches!(replica_identity, b'd' | b'i' | b'f')
    {
        return Err(refusal(
            "follow-target: relation does not match the admitted owned table plan",
        ));
    }
    require_setting(target, "row_security", "off", io)?;
    require_setting(target, "session_replication_role", "replica", io)?;
    require_setting(target, "transaction_read_only", "off", io)?;
    require_setting(target, "transaction_isolation", "read committed", io)?;
    let quoted = qualified(target, schema, name)?;
    command(
        target,
        &format!("LOCK TABLE ONLY {quoted} IN ROW EXCLUSIVE MODE"),
        "LOCK TABLE",
        io,
    )?;
    let oid = owned.target_oid;
    let lookup=target.query(&format!("SELECT c.oid,c.relnamespace,c.relowner,n.nspname,c.relname,c.relkind='r' AND c.relpersistence='p' AND am.amname='heap' AND am.oid<16384 AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid AND t.tgenabled IN ('A','R')) AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=c.oid AND r.ev_enabled IN ('A','R')),c.relreplident FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_am am ON am.oid=c.relam WHERE c.oid={oid}"),io)?;
    if lookup.rows() != 1
        || lookup.columns()? != 7
        || text(&lookup, 0, 0)? != oid.to_string()
        || text(&lookup, 0, 1)? != owned.target_namespace_oid.to_string()
        || text(&lookup, 0, 2)? != owned.target_owner_oid.to_string()
        || text(&lookup, 0, 3)? != schema
        || text(&lookup, 0, 4)? != name
        || lookup.cell(0, 5)? != Some(b"t")
        || lookup.cell(0, 6)? != Some(std::slice::from_ref(replica_identity))
    {
        return Err(refusal(
            "follow-target: target table identity or execution policy changed",
        ));
    }
    let (identity_modes, partition_identity_modes) =
        super::clone::clone_identity_modes(target, oid, &owned.table.all_columns, io)?;
    super::clone::require_target_identity_modes(
        &owned.table,
        target.server_version()? / 10_000,
        &identity_modes,
        partition_identity_modes.as_deref(),
    )?;
    require_follow_execution(target, oid, io)?;
    let identity_keys = identity_columns(
        target,
        oid,
        std::str::from_utf8(std::slice::from_ref(replica_identity))
            .map_err(|_| refusal("follow-target: invalid replica identity"))?,
        io,
    )?;
    let attributes=target.query(&format!("WITH RECURSIVE types AS (SELECT a.attnum,a.atttypid AS base,0 AS depth FROM pg_catalog.pg_attribute a WHERE a.attrelid={oid} AND a.attnum>0 AND NOT a.attisdropped UNION ALL SELECT x.attnum,t.typbasetype,x.depth+1 FROM types x JOIN pg_catalog.pg_type t ON t.oid=x.base WHERE t.typtype='d' AND x.depth<32) SELECT a.attname,a.atttypid,a.atttypmod,a.attgenerated,a.attidentity,n.nspname,t.typname,a.attnotnull FROM pg_catalog.pg_attribute a JOIN types x ON x.attnum=a.attnum JOIN pg_catalog.pg_type t ON t.oid=x.base AND t.typtype<>'d' JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE a.attrelid={oid} AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum LIMIT 1601"),io)?;
    if attributes.columns()? != 8
        || attributes.rows() != columns.len()
        || columns.is_empty()
        || columns.len() > 1600
    {
        return Err(refusal(
            "follow-target: published column layout differs from target",
        ));
    }
    let mut target_type_oids = Vec::with_capacity(columns.len());
    for (row, column) in columns.iter().enumerate() {
        let actual = cdc_object_id(text(&attributes, row, 1)?)?;
        let modifier = text(&attributes, row, 2)?
            .parse::<i32>()
            .map_err(|_| refusal("follow-target: invalid type modifier"))?;
        let custom = binding
            .source_types
            .iter()
            .find(|item| item.oid == column.type_oid);
        let type_matches = if column.type_oid < 16384 {
            actual == column.type_oid
        } else {
            custom.is_some_and(|item| {
                text(&attributes, row, 5).ok() == Some(item.schema.as_str())
                    && text(&attributes, row, 6).ok() == Some(item.name.as_str())
            })
        };
        if text(&attributes, row, 0)? != column.name
            || !text(&attributes, row, 3)?.is_empty()
            || modifier != column.type_mod
            || !type_matches
        {
            return Err(refusal(
                "follow-target: published column type, name, modifier, or generation differs",
            ));
        }
        if column.key != (*replica_identity == b'f' || identity_keys.contains(&(row + 1))) {
            return Err(refusal(
                "follow-target: published replica identity differs from reconstructed target",
            ));
        }
        if column.key && *replica_identity != b'f' && attributes.cell(row, 7)? != Some(b"t") {
            return Err(refusal("follow-target: replica identity key is nullable"));
        }
        target_type_oids.push(actual);
    }
    Ok(FollowRelation {
        relation: expected.clone(),
        target_type_oids,
        identity_modes,
    })
}

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{CloneTable, CopyGeneration, RunId};

    fn table() -> CloneTable {
        CloneTable {
            source_oid: 42,
            name: QualifiedTable {
                schema: "public".into(),
                table: "items".into(),
            },
            writable_columns: vec!["id".into(), "value".into()],
            all_columns: vec!["id".into(), "value".into()],
            identity_modes: vec![0, 0],
            partition_identity_modes: None,
        }
    }

    fn live_fixture(label: &str) -> (PgConnection, IoControl, String) {
        let context = std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned harness required");
        let context: serde_json::Value =
            serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap();
        assert_eq!(
            std::fs::read_to_string(
                std::path::Path::new(context["root"].as_str().unwrap()).join(".e1-owner")
            )
            .unwrap(),
            context["token"].as_str().unwrap()
        );
        let io = IoControl {
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let info =
            crate::contracts::Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_SOURCE").unwrap())
                .unwrap();
        let mut connection = PgConnection::connect(&info, &io).unwrap();
        configure_session(&mut connection, &io).unwrap();
        command(&mut connection, "BEGIN", "BEGIN", &io).unwrap();
        let schema = format!("identity_{label}_{}", std::process::id());
        connection
            .query(&format!("CREATE SCHEMA {schema}"), &io)
            .unwrap();
        (connection, io, schema)
    }

    fn live_source_plan(connection: &mut PgConnection, schema: &str, io: &IoControl) -> ClonePlan {
        let source = read_clone_source(connection, io).unwrap();
        let tables = inspect_clone_tables(connection, &[], io)
            .unwrap()
            .into_iter()
            .filter(|t| t.name.schema == schema)
            .collect();
        ClonePlan {
            target: crate::contracts::CloneTarget {
                identity: SourceId {
                    system_identifier: source.identity.system_identifier,
                    database_oid: source.identity.database_oid + 1,
                },
                major: source.major,
            },
            source,
            tables,
            generation: CopyGeneration(1),
            sequences: vec![],
            policy: crate::contracts::RestorePolicy::default(),
        }
    }

    #[test]
    #[ignore = "requires owned PostgreSQL harness"]
    fn live_identity_source_mode_drift_refuses_before_resources() {
        let (mut connection, io, schema) = live_fixture("source_drift");
        connection.query(&format!("CREATE TABLE {schema}.items(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, body text)"), &io).unwrap();
        for sql in [
            format!(
                "CREATE TABLE {schema}.root(id integer GENERATED ALWAYS AS IDENTITY, body integer) PARTITION BY RANGE (body)"
            ),
            format!(
                "CREATE TABLE {schema}.leaf PARTITION OF {schema}.root FOR VALUES FROM (0) TO (100)"
            ),
            format!("ALTER TABLE {schema}.leaf REPLICA IDENTITY FULL"),
        ] {
            connection.query(&sql, &io).unwrap();
        }
        let plan = live_source_plan(&mut connection, &schema, &io);
        inspect_source_tables(&mut connection, &plan, &io).unwrap();
        for mode in ["BY DEFAULT", "ALWAYS"] {
            connection
                .query(
                    &format!("ALTER TABLE {schema}.items ALTER COLUMN id SET GENERATED {mode}"),
                    &io,
                )
                .unwrap();
            let mut expected = plan.clone();
            if mode == "ALWAYS" {
                // The prior BY DEFAULT plan must also refuse a return to ALWAYS.
                expected.tables[0].identity_modes[0] = b'd';
            }
            let error = inspect_source_tables(&mut connection, &expected, &io).unwrap_err();
            assert!(error.message.contains("identity"), "{error:?}");
        }
        connection
            .query(
                &format!("ALTER TABLE {schema}.root ALTER COLUMN id SET GENERATED BY DEFAULT"),
                &io,
            )
            .unwrap();
        assert!(
            inspect_source_tables(&mut connection, &plan, &io)
                .unwrap_err()
                .message
                .contains("identity")
        );
        command(&mut connection, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires owned PostgreSQL harness"]
    fn live_identity_internal_replica_callbacks_refuse_both_endpoints() {
        let (mut connection, io, schema) = live_fixture("callbacks");
        for sql in [
            format!("CREATE TABLE {schema}.parent(id integer PRIMARY KEY)"),
            format!(
                "CREATE TABLE {schema}.child(id integer PRIMARY KEY, parent_id integer REFERENCES {schema}.parent(id))"
            ),
            format!("INSERT INTO {schema}.parent VALUES (1)"),
            format!("INSERT INTO {schema}.child VALUES (1,1)"),
        ] {
            connection.query(&sql, &io).unwrap();
        }
        let plan = live_source_plan(&mut connection, &schema, &io);
        inspect_source_tables(&mut connection, &plan, &io).unwrap();
        let table = plan
            .tables
            .iter()
            .find(|t| t.name.table == "child")
            .unwrap();
        let oid = table.source_oid;
        connection
            .query(
                &format!("ALTER TABLE {schema}.child REPLICA IDENTITY FULL"),
                &io,
            )
            .unwrap();
        let binding = live_binding(&plan);
        let (owned, relation) = live_owned_relation(&mut connection, &plan, "child", &io);
        connection
            .query("SET LOCAL session_replication_role=replica", &io)
            .unwrap();
        preflight_follow_relation(&mut connection, &relation, &binding, &owned, &io).unwrap();
        // An origin-only FK stays disabled under the admitted replica apply role.
        connection
            .query(&format!("INSERT INTO {schema}.child VALUES (2,99)"), &io)
            .unwrap();
        let trigger = connection.query(&format!("SELECT tgname FROM pg_catalog.pg_trigger WHERE tgrelid={oid} AND tgisinternal ORDER BY oid LIMIT 1"), &io).unwrap();
        assert_eq!(trigger.rows(), 1);
        let trigger = connection
            .quote_identifier(text(&trigger, 0, 0).unwrap())
            .unwrap();
        for mode in ["ALWAYS", "REPLICA"] {
            connection
                .query(
                    &format!("ALTER TABLE {schema}.child ENABLE {mode} TRIGGER {trigger}"),
                    &io,
                )
                .unwrap();
            let error = inspect_source_tables(&mut connection, &plan, &io).unwrap_err();
            assert!(error.message.contains("trigger"), "{error:?}");
            assert!(inspect_clone_tables(&mut connection, &[], &io).is_err());
            assert!(
                preflight_follow_relation(&mut connection, &relation, &binding, &owned, &io)
                    .is_err()
            );
            connection
                .query(
                    &format!("ALTER TABLE {schema}.child ENABLE TRIGGER {trigger}"),
                    &io,
                )
                .unwrap();
        }
        connection.query(&format!("CREATE FUNCTION {schema}.callback() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$"),&io).unwrap();
        connection.query(&format!("CREATE TRIGGER user_callback BEFORE INSERT ON {schema}.child FOR EACH ROW EXECUTE FUNCTION {schema}.callback()"),&io).unwrap();
        inspect_source_tables(&mut connection, &plan, &io).unwrap();
        preflight_follow_relation(&mut connection, &relation, &binding, &owned, &io).unwrap();
        for mode in ["ALWAYS", "REPLICA"] {
            connection
                .query(
                    &format!("ALTER TABLE {schema}.child ENABLE {mode} TRIGGER user_callback"),
                    &io,
                )
                .unwrap();
            assert!(inspect_source_tables(&mut connection, &plan, &io).is_err());
            assert!(
                preflight_follow_relation(&mut connection, &relation, &binding, &owned, &io)
                    .is_err()
            );
            connection
                .query(
                    &format!("ALTER TABLE {schema}.child ENABLE TRIGGER user_callback"),
                    &io,
                )
                .unwrap();
        }
        assert_eq!(
            connection
                .query(
                    &format!("SELECT id,parent_id FROM {schema}.child ORDER BY id"),
                    &io
                )
                .unwrap()
                .rows(),
            2
        );
        assert_eq!(
            connection
                .query(&format!("SELECT id FROM {schema}.parent"), &io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"1".as_slice())
        );
        command(&mut connection, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    fn live_binding(plan: &ClonePlan) -> FollowBinding {
        FollowBinding {
            slot: crate::state::SourceSlotReceipt {
                intent: crate::state::SourceSlotIntent {
                    binding: crate::state::SourceSlotBinding::new(
                        crate::identity::StreamId {
                            run: RunId([3; 16]),
                            source: plan.source.identity,
                            generation: 1,
                        },
                        "identity_test_slot".into(),
                        crate::cdc::contracts::Plugin::Pgoutput,
                        1,
                    )
                    .unwrap(),
                    nonce: [4; 16],
                },
                consistent_point: SourceEndLsn(1),
                snapshot_token: "identity-test".into(),
            },
            publication: PublicationReceipt {
                name: "identity_test_publication".into(),
                oid: 42,
                owner_oid: 43,
            },
            source_timeline: plan.source.timeline,
            copy_generation: plan.generation,
            source_types: vec![],
        }
    }

    fn live_owned_relation(
        connection: &mut PgConnection,
        plan: &ClonePlan,
        table_name: &str,
        io: &IoControl,
    ) -> (OwnedCopyTable, Event) {
        let table = plan
            .tables
            .iter()
            .find(|t| t.name.table == table_name)
            .unwrap()
            .clone();
        let oid = table.source_oid;
        let object = connection
            .query(
                &format!("SELECT relnamespace,relowner FROM pg_catalog.pg_class WHERE oid={oid}"),
                io,
            )
            .unwrap();
        let attributes = connection.query(&format!("SELECT attname,atttypid,atttypmod FROM pg_catalog.pg_attribute WHERE attrelid={oid} AND attnum>0 AND NOT attisdropped ORDER BY attnum"), io).unwrap();
        let event = Event::Relation {
            key: RelationKey { oid, generation: 1 },
            schema: table.name.schema.clone(),
            name: table.name.table.clone(),
            replica_identity: b'f',
            columns: (0..attributes.rows())
                .map(|row| CdcColumn {
                    name: text(&attributes, row, 0).unwrap().into(),
                    type_oid: cdc_object_id(text(&attributes, row, 1).unwrap()).unwrap(),
                    type_mod: text(&attributes, row, 2).unwrap().parse().unwrap(),
                    key: true,
                })
                .collect(),
        };
        (
            OwnedCopyTable {
                table,
                target_oid: oid,
                target_namespace_oid: cdc_object_id(text(&object, 0, 0).unwrap()).unwrap(),
                target_owner_oid: cdc_object_id(text(&object, 0, 1).unwrap()).unwrap(),
                generation: plan.generation,
            },
            event,
        )
    }

    #[test]
    #[ignore = "requires owned PostgreSQL harness"]
    fn live_identity_target_modes_and_partition_normalization_refuse_drift() {
        let (mut connection, io, schema) = live_fixture("target_modes");
        for sql in [
            format!(
                "CREATE TABLE {schema}.items(id integer GENERATED ALWAYS AS IDENTITY, body integer)"
            ),
            format!("ALTER TABLE {schema}.items REPLICA IDENTITY FULL"),
            format!(
                "CREATE TABLE {schema}.root(id integer GENERATED ALWAYS AS IDENTITY, body integer) PARTITION BY RANGE (body)"
            ),
            format!(
                "CREATE TABLE {schema}.leaf PARTITION OF {schema}.root FOR VALUES FROM (0) TO (100)"
            ),
            format!("ALTER TABLE {schema}.leaf REPLICA IDENTITY FULL"),
        ] {
            connection.query(&sql, &io).unwrap();
        }
        let mut plan = live_source_plan(&mut connection, &schema, &io);
        plan.target = read_clone_target(&mut connection, &io).unwrap();
        let objects = clone_object_inventory(&mut connection, &io).unwrap();
        reconcile_clone_tables(&mut connection, &plan, &objects, &io).unwrap();
        let binding = live_binding(&plan);
        connection
            .query("SET LOCAL session_replication_role=replica", &io)
            .unwrap();
        let (owned, relation) = live_owned_relation(&mut connection, &plan, "items", &io);
        assert_eq!(
            preflight_follow_relation(&mut connection, &relation, &binding, &owned, &io)
                .unwrap()
                .identity_modes,
            [b'a', 0]
        );
        for mode in ["BY DEFAULT", "ALWAYS"] {
            connection
                .query(
                    &format!("ALTER TABLE {schema}.items ALTER COLUMN id SET GENERATED {mode}"),
                    &io,
                )
                .unwrap();
            let mut expected = owned.clone();
            if mode == "ALWAYS" {
                expected.table.identity_modes[0] = b'd';
            }
            assert!(
                preflight_follow_relation(&mut connection, &relation, &binding, &expected, &io)
                    .unwrap_err()
                    .message
                    .contains("identity")
            );
            let mut expected_plan = plan.clone();
            *expected_plan
                .tables
                .iter_mut()
                .find(|t| t.name.table == "items")
                .unwrap() = expected.table;
            assert!(
                reconcile_clone_tables(&mut connection, &expected_plan, &objects, &io).is_err()
            );
        }
        let (mut leaf, relation) = live_owned_relation(&mut connection, &plan, "leaf", &io);
        // A valid PG16 source leaf maps to root modes only on target PG18.
        leaf.table.identity_modes.fill(0);
        let major = connection.server_version().unwrap() / 10_000;
        let expected = if major == 18 {
            vec![b'a', 0]
        } else {
            vec![0, 0]
        };
        assert_eq!(
            preflight_follow_relation(&mut connection, &relation, &binding, &leaf, &io)
                .unwrap()
                .identity_modes,
            expected
        );
        connection
            .query(
                &format!("ALTER TABLE {schema}.root ALTER COLUMN id SET GENERATED BY DEFAULT"),
                &io,
            )
            .unwrap();
        assert!(
            preflight_follow_relation(&mut connection, &relation, &binding, &leaf, &io).is_err()
        );
        command(&mut connection, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires owned PostgreSQL harness"]
    fn live_identity_cross_major_independent_leaf_refuses_before_resources() {
        let (mut connection, io, schema) = live_fixture("cross_major");
        for sql in [
            format!(
                "CREATE TABLE {schema}.root(id integer GENERATED ALWAYS AS IDENTITY, body integer) PARTITION BY RANGE (body)"
            ),
            format!(
                "CREATE TABLE {schema}.leaf PARTITION OF {schema}.root FOR VALUES FROM (0) TO (100)"
            ),
            format!("ALTER TABLE {schema}.leaf REPLICA IDENTITY FULL"),
        ] {
            connection.query(&sql, &io).unwrap();
        }
        let mut normal = live_source_plan(&mut connection, &schema, &io);
        normal.target.major = 18;
        inspect_source_tables(&mut connection, &normal, &io).unwrap();
        let major = connection.server_version().unwrap() / 10_000;
        if major == 18 {
            connection
                .query("SAVEPOINT incompatible_leaf", &io)
                .unwrap();
            let error = connection.query(&format!("ALTER TABLE {schema}.leaf ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY"), &io).err().expect("stock PG18 accepted independent leaf identity");
            assert!(error.message.contains("SQLSTATE 42P16"), "{error:?}");
            connection
                .query("ROLLBACK TO SAVEPOINT incompatible_leaf", &io)
                .unwrap();
        } else {
            connection.query(&format!("ALTER TABLE {schema}.leaf ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY"), &io).unwrap();
            let mut plan = live_source_plan(&mut connection, &schema, &io);
            inspect_source_tables(&mut connection, &plan, &io).unwrap();
            let binding = live_binding(&plan);
            let (owned, relation) = live_owned_relation(&mut connection, &plan, "leaf", &io);
            connection
                .query("SET LOCAL session_replication_role=replica", &io)
                .unwrap();
            assert_eq!(
                preflight_follow_relation(&mut connection, &relation, &binding, &owned, &io)
                    .unwrap()
                    .identity_modes,
                [b'd', 0]
            );
            plan.target.major = 18;
            let before = connection.query("SELECT (SELECT count(*) FROM pg_catalog.pg_publication),(SELECT count(*) FROM pg_catalog.pg_replication_slots)", &io).unwrap();
            let error = inspect_source_tables(&mut connection, &plan, &io).unwrap_err();
            assert!(
                error.message.contains("independent")
                    && error.message.contains("16")
                    && error.message.contains("18"),
                "{error:?}"
            );
            let after = connection.query("SELECT (SELECT count(*) FROM pg_catalog.pg_publication),(SELECT count(*) FROM pg_catalog.pg_replication_slots)", &io).unwrap();
            for column in 0..2 {
                assert_eq!(
                    before.cell(0, column).unwrap(),
                    after.cell(0, column).unwrap()
                );
            }
        }
        command(&mut connection, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    fn retention_refuses_known_wal_loss_and_snapshot_expiry_policy() {
        retention_policy(16, "logical", "-1", Some("-1"), None).unwrap();
        retention_policy(18, "logical", "-1", None, Some("0")).unwrap();
        for (major, wal, keep, old, idle) in [
            (16, "replica", "-1", Some("-1"), None),
            (16, "logical", "0", Some("-1"), None),
            (18, "logical", "1024", None, Some("0")),
            (16, "logical", "-1", Some("0"), None),
            (18, "logical", "-1", None, Some("60")),
            (18, "logical", "-1", None, None),
            (17, "logical", "-1", None, Some("0")),
        ] {
            assert!(retention_policy(major, wal, keep, old, idle).is_err());
        }
    }

    #[test]
    fn membership_requires_exact_physical_oid_name_and_column_inventory() {
        let table = table();
        let actual = vec![(42, table.name.clone(), table.all_columns.clone())];
        exact_membership(std::slice::from_ref(&table), &actual).unwrap();
        assert!(exact_membership(std::slice::from_ref(&table), &[]).is_err());
        let mut changed = actual.clone();
        changed[0].0 = 43;
        assert!(exact_membership(std::slice::from_ref(&table), &changed).is_err());
        changed = actual.clone();
        changed[0].1.table = "other".into();
        assert!(exact_membership(std::slice::from_ref(&table), &changed).is_err());
        changed = actual.clone();
        changed[0].2.reverse();
        assert!(exact_membership(std::slice::from_ref(&table), &changed).is_err());
        changed = actual.clone();
        changed.push(actual[0].clone());
        assert!(exact_membership(std::slice::from_ref(&table), &changed).is_err());
        exact_membership(&[], &[]).unwrap();
    }

    #[test]
    fn native_resource_names_are_run_generation_bound_and_bounded() {
        let run = RunId([0xab; 16]);
        let first = resource_name("slot", run, CopyGeneration(1)).unwrap();
        let second = resource_name("slot", run, CopyGeneration(2)).unwrap();
        assert!(
            first.len() <= 63
                && first
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        );
        assert_ne!(first, second);
        assert_ne!(
            first,
            resource_name("publication", run, CopyGeneration(1)).unwrap()
        );
        assert!(resource_name("slot", RunId([0; 16]), CopyGeneration(1)).is_err());
        assert!(resource_name("slot", run, CopyGeneration(0)).is_err());
    }

    #[test]
    fn follow_preflight_refuses_any_generated_column() {
        reject_generated(std::slice::from_ref(&table())).unwrap();
        let mut generated = table();
        generated.writable_columns.pop();
        assert!(
            reject_generated(&[generated])
                .unwrap_err()
                .message
                .contains("generated")
        );
    }

    #[test]
    fn lsn_parser_refuses_absent_malformed_and_zero_slot_evidence() {
        assert_eq!(
            slot_lsn("1/00000002").unwrap(),
            crate::identity::SourceEndLsn(0x1_00000002)
        );
        for invalid in [
            "",
            "0/0",
            "1",
            "1/2/3",
            "-1/2",
            "100000000/1",
            "1/G",
            " 1/2",
        ] {
            assert!(slot_lsn(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn slot_owner_requires_inactive_or_the_exact_runtime_pid() {
        require_slot_owner(None, None).unwrap();
        require_slot_owner(None, Some(42)).unwrap();
        require_slot_owner(Some(42), Some(42)).unwrap();
        for (active, expected) in [
            (Some(42), None),
            (Some(42), Some(43)),
            (None, Some(0)),
            (Some(-1), Some(-1)),
        ] {
            assert!(require_slot_owner(active, expected).is_err());
        }
    }
}
