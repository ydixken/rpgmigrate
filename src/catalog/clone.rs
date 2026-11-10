use super::*;
use crate::contracts::{
    CloneCopyPolicy, ClonePlan, CloneSource, CloneTable, CloneTableCopy, CloneTarget,
    CopyGeneration, CopyPart, MAX_COPY_PARTS, OwnedCopyTable, OwnedObject, RestorePolicy,
};

const MAX_TABLES: usize = 4096;
const MAX_OBJECTS: usize = 65_536;

fn admitted_relation(
    kind: &str,
    persistence: &str,
    access_method: &str,
    extension_owned: bool,
    unusual_trigger: bool,
) -> bool {
    !extension_owned
        && !unusual_trigger
        && persistence == "p"
        && match kind {
            "r" => access_method == "heap",
            "p" | "v" | "S" => true,
            _ => false,
        }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BinaryColumn {
    name: String,
    type_oid: u32,
    namespace_oid: u32,
    type_name: String,
    kind: String,
    modifier: i32,
    scalar: bool,
    send_oid: u32,
    receive_oid: u32,
    send_name: String,
    receive_name: String,
    send_builtin: bool,
    receive_builtin: bool,
    extension_owned: bool,
}

fn binary_columns_compatible(
    source: &[BinaryColumn],
    target: &[BinaryColumn],
    source_encodings: (&str, &str),
    target_encodings: (&str, &str),
) -> bool {
    if source.is_empty()
        || source != target
        || source_encodings != target_encodings
        || source_encodings.0.is_empty()
        || source_encodings.0 != source_encodings.1
    {
        return false;
    }
    source.iter().all(|column| {
        // Both majors pin these scalar and binary I/O identities in pg_type.dat/pg_proc.dat.
        let (oid, send, receive, io_name) = match column.type_name.as_str() {
            "bool" => (16, 2437, 2436, "bool"),
            "bytea" => (17, 2413, 2412, "bytea"),
            "int2" => (21, 2405, 2404, "int2"),
            "int4" => (23, 2407, 2406, "int4"),
            "int8" => (20, 2409, 2408, "int8"),
            "float4" => (700, 2425, 2424, "float4"),
            "float8" => (701, 2427, 2426, "float8"),
            "numeric" => (1700, 2461, 2460, "numeric_"),
            "text" => (25, 2415, 2414, "text"),
            "varchar" => (1043, 2433, 2432, "varchar"),
            "bpchar" => (1042, 2431, 2430, "bpchar"),
            "date" => (1082, 2469, 2468, "date_"),
            "time" => (1083, 2471, 2470, "time_"),
            "timestamp" => (1114, 2475, 2474, "timestamp_"),
            "timestamptz" => (1184, 2477, 2476, "timestamptz_"),
            "interval" => (1186, 2479, 2478, "interval_"),
            "uuid" => (2950, 2962, 2961, "uuid_"),
            "json" => (114, 324, 323, "json_"),
            "jsonb" => (3802, 3803, 3805, "jsonb_"),
            _ => return false,
        };
        (column.type_oid, column.send_oid, column.receive_oid) == (oid, send, receive)
            && column.send_name == format!("{io_name}send")
            && column.receive_name == format!("{io_name}recv")
            && column.namespace_oid == 11
            && column.kind == "b"
            && column.scalar
            && column.send_builtin
            && column.receive_builtin
            && !column.extension_owned
    })
}

fn require_planning_transactions(source: &PgConnection, target: &PgConnection) -> Result<()> {
    if source.transaction_status()? != TransactionStatus::InTransaction
        || target.transaction_status()? != TransactionStatus::InTransaction
    {
        return Err(refusal(
            "COPY planning requires the caller's healthy source snapshot and target transaction",
        ));
    }
    Ok(())
}

fn binary_columns(
    connection: &mut PgConnection,
    oid: u32,
    io: &IoControl,
) -> Result<Vec<BinaryColumn>> {
    let result = connection.query(&format!(r#"SELECT a.attname,t.oid,t.typnamespace,t.typname,t.typtype,a.atttypmod,
 t.typelem=0 AND t.typbasetype=0 AND t.typrelid=0,t.typsend,t.typreceive,
 COALESCE(s.proname::pg_catalog.text,''),COALESCE(r.proname::pg_catalog.text,''),
 COALESCE(s.pronamespace=11 AND s.prolang=12 AND s.prokind='f' AND s.prosrc=s.proname::pg_catalog.text,false),
 COALESCE(r.pronamespace=11 AND r.prolang=12 AND r.prokind='f' AND r.prosrc=r.proname::pg_catalog.text,false),
 EXISTS(SELECT 1 FROM pg_catalog.pg_depend d WHERE d.deptype='e' AND
 ((d.classid='pg_catalog.pg_type'::pg_catalog.regclass AND d.objid=t.oid) OR
 (d.classid='pg_catalog.pg_proc'::pg_catalog.regclass AND d.objid IN (t.typsend,t.typreceive))))
FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
LEFT JOIN pg_catalog.pg_proc s ON s.oid=t.typsend LEFT JOIN pg_catalog.pg_proc r ON r.oid=t.typreceive
WHERE a.attrelid={oid} AND a.attnum>0 AND NOT a.attisdropped AND a.attgenerated=''
ORDER BY a.attnum LIMIT 1601"#), io)?;
    if result.rows() > 1600 || result.columns()? != 14 {
        return Err(refusal(
            "binary COPY requires a bounded complete writable column layout",
        ));
    }
    (0..result.rows())
        .map(|row| {
            let number = |column| {
                text(&result, row, column)?
                    .parse::<u32>()
                    .map_err(|_| refusal("binary COPY catalog returned an invalid identity"))
            };
            Ok(BinaryColumn {
                name: text(&result, row, 0)?.into(),
                type_oid: number(1)?,
                namespace_oid: number(2)?,
                type_name: text(&result, row, 3)?.into(),
                kind: text(&result, row, 4)?.into(),
                modifier: text(&result, row, 5)?
                    .parse()
                    .map_err(|_| refusal("binary COPY catalog returned an invalid typmod"))?,
                scalar: text(&result, row, 6)? == "t",
                send_oid: number(7)?,
                receive_oid: number(8)?,
                send_name: text(&result, row, 9)?.into(),
                receive_name: text(&result, row, 10)?.into(),
                send_builtin: text(&result, row, 11)? == "t",
                receive_builtin: text(&result, row, 12)? == "t",
                extension_owned: text(&result, row, 13)? != "f",
            })
        })
        .collect()
}

fn locked_clone_oid(
    connection: &mut PgConnection,
    table: &CloneTable,
    io: &IoControl,
) -> Result<u32> {
    require_copy_name(&table.name)?;
    let quoted = qualified(connection, &table.name.schema, &table.name.table)?;
    command(
        connection,
        &format!("LOCK TABLE ONLY {quoted} IN ACCESS SHARE MODE"),
        "LOCK TABLE",
        io,
    )?;
    let schema = connection.quote_literal(&table.name.schema)?;
    let name = connection.quote_literal(&table.name.table)?;
    let result = connection.query(&format!("SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_am a ON a.oid=c.relam WHERE n.nspname={schema} AND c.relname={name} AND c.relkind='r' AND c.relpersistence='p' AND a.amname='heap' AND a.oid<16384 LIMIT 2"), io)?;
    if result.rows() != 1 || result.columns()? != 1 {
        return Err(refusal(
            "COPY planning requires one exact locked physical heap",
        ));
    }
    cdc_object_id(text(&result, 0, 0)?)
}

pub(crate) fn binary_copy_compatible(
    source: &mut PgConnection,
    target: &mut PgConnection,
    table: &CloneTable,
    io: &IoControl,
) -> Result<bool> {
    require_planning_transactions(source, target)?;
    if locked_clone_oid(source, table, io)? != table.source_oid {
        return Err(refusal(
            "binary COPY source table identity differs from the snapshot plan",
        ));
    }
    let target_oid = locked_clone_oid(target, table, io)?;
    let source_columns = binary_columns(source, table.source_oid, io)?;
    let target_columns = binary_columns(target, target_oid, io)?;
    if source_columns
        .iter()
        .map(|c| &c.name)
        .ne(table.writable_columns.iter())
    {
        return Err(refusal(
            "binary COPY source writable columns differ from the snapshot plan",
        ));
    }
    let source_server = setting(source, "server_encoding", io)?;
    let source_client = setting(source, "client_encoding", io)?;
    let target_server = setting(target, "server_encoding", io)?;
    let target_client = setting(target, "client_encoding", io)?;
    Ok(binary_columns_compatible(
        &source_columns,
        &target_columns,
        (&source_server, &source_client),
        (&target_server, &target_client),
    ))
}

pub(crate) fn plan_copy_parts(
    relation_bytes: u64,
    block_size: u32,
    threshold_bytes: u64,
    max_parts: u32,
) -> Result<Vec<CopyPart>> {
    if block_size == 0 || max_parts == 0 || max_parts > i32::MAX as u32 {
        return Err(refusal(
            "COPY parts require a positive block size and bounded part maximum",
        ));
    }
    let blocks = relation_bytes.div_ceil(u64::from(block_size));
    if blocks > u64::from(u32::MAX) {
        return Err(refusal(
            "COPY physical table exceeds PostgreSQL block bounds",
        ));
    }
    let requested = if threshold_bytes == 0 || relation_bytes <= threshold_bytes {
        1
    } else {
        relation_bytes
            .div_ceil(threshold_bytes)
            .min(u64::from(max_parts))
            .min(blocks)
    };
    let width = blocks.div_ceil(requested).max(1);
    let count = blocks.div_ceil(width).max(1);
    if count > MAX_COPY_PARTS as u64 {
        return Err(refusal("COPY exceeds the 4096 actual part bound"));
    }
    let count = count as u32;
    Ok((0..count)
        .map(|index| CopyPart {
            number: index + 1,
            count,
            start_block: (u64::from(index) * width) as u32,
            end_block: (index + 1 < count).then(|| (u64::from(index + 1) * width) as u32),
        })
        .collect())
}

pub(crate) fn provisional_copy_tables(tables: &[CloneTable]) -> Result<Vec<CloneTableCopy>> {
    if tables.len() > MAX_COPY_PARTS {
        return Err(refusal("COPY exceeds the 4096 actual part bound"));
    }
    let mut entries = tables
        .iter()
        .map(|table| CloneTableCopy {
            source_oid: table.source_oid,
            format: CopyFormat::Text,
            parts: vec![CopyPart {
                number: 1,
                count: 1,
                start_block: 0,
                end_block: None,
            }],
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by_key(|entry| entry.source_oid);
    let mut previous = 0;
    for entry in &entries {
        if entry.source_oid <= previous {
            return Err(refusal(
                "COPY profile requires unique nonzero source table identities",
            ));
        }
        previous = entry.source_oid;
    }
    Ok(entries)
}

pub(crate) fn resolve_clone_copy_tables(
    source: &mut PgConnection,
    target: &mut PgConnection,
    plan: &ClonePlan,
    io: &IoControl,
) -> Result<Vec<CloneTableCopy>> {
    require_planning_transactions(source, target)?;
    if read_clone_source(source, io)? != plan.source
        || read_clone_target(target, io)? != plan.target
    {
        return Err(refusal("COPY execution planning endpoint identity differs"));
    }
    let mut entries = provisional_copy_tables(&plan.tables)?;
    let block_size = setting(source, "block_size", io)?
        .parse::<u32>()
        .map_err(|_| refusal("COPY planning returned an invalid physical block size"))?;
    let by_oid = plan
        .tables
        .iter()
        .map(|table| (table.source_oid, table))
        .collect::<BTreeMap<_, _>>();
    let mut total = 0usize;
    for entry in &mut entries {
        let table = by_oid[&entry.source_oid];
        if locked_clone_oid(source, table, io)? != table.source_oid {
            return Err(refusal(
                "COPY physical table identity differs from its snapshot plan",
            ));
        }
        let result = source.query(
            &format!(
                "SELECT pg_catalog.pg_relation_size({}::pg_catalog.oid, 'main')",
                table.source_oid
            ),
            io,
        )?;
        if result.rows() != 1 || result.columns()? != 1 {
            return Err(refusal(
                "COPY physical storage measurement is absent or ambiguous",
            ));
        }
        let bytes = text(&result, 0, 0)?
            .parse::<u64>()
            .map_err(|_| refusal("COPY physical storage measurement is invalid"))?;
        let parts = plan_copy_parts(
            bytes,
            block_size,
            plan.copy_policy.split_tables_larger_than,
            plan.copy_policy.split_max_parts,
        )?;
        total = total
            .checked_add(parts.len())
            .ok_or_else(|| refusal("COPY actual part count overflow"))?;
        if total > MAX_COPY_PARTS {
            return Err(refusal("COPY exceeds the 4096 actual part bound"));
        }
        entry.parts = parts;
        entry.format = if plan.copy_policy.use_copy_binary
            && binary_copy_compatible(source, target, table, io)?
        {
            CopyFormat::Binary
        } else {
            CopyFormat::Text
        };
    }
    Ok(entries)
}

fn clone_identity(connection: &mut PgConnection, io: &IoControl) -> Result<(SourceId, u32, u32)> {
    let major = connection.server_version()? / 10_000;
    if !matches!(major, 16 | 18) {
        return Err(refusal("clone requires PostgreSQL 16 or 18"));
    }
    let result = connection.query("SELECT c.system_identifier::pg_catalog.text, d.oid, k.timeline_id, pg_catalog.pg_is_in_recovery(), pg_catalog.pg_backend_pid() FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_control_checkpoint() k CROSS JOIN pg_catalog.pg_database d WHERE d.datname OPERATOR(pg_catalog.=) pg_catalog.current_database()",io)?;
    if result.rows() != 1 || result.columns()? != 5 || text(&result, 0, 3)? != "f" {
        return Err(refusal(
            "clone requires a primary database with a positive control identity",
        ));
    }
    let system = text(&result, 0, 0)?
        .parse::<i64>()
        .map_err(|_| refusal("clone returned an invalid system identity"))?;
    if system == 0 || system.to_string() != text(&result, 0, 0)? {
        return Err(refusal("clone returned an invalid system identity"));
    }
    if cdc_object_id(text(&result, 0, 4)?)? != connection.backend_pid()? {
        return Err(refusal(
            "clone backend identity differs from the physical connection",
        ));
    }
    Ok((
        SourceId {
            system_identifier: system as u64,
            database_oid: cdc_object_id(text(&result, 0, 1)?)?,
        },
        cdc_object_id(text(&result, 0, 2)?)?,
        major,
    ))
}

pub(crate) fn read_clone_source(
    connection: &mut PgConnection,
    io: &IoControl,
) -> Result<CloneSource> {
    let (identity, timeline, major) = clone_identity(connection, io)?;
    Ok(CloneSource {
        identity,
        timeline,
        major,
    })
}

pub(crate) fn read_clone_target(
    connection: &mut PgConnection,
    io: &IoControl,
) -> Result<CloneTarget> {
    let (identity, _, major) = clone_identity(connection, io)?;
    Ok(CloneTarget { identity, major })
}

const OBJECTS_SQL: &str = r#"
WITH known AS (
 SELECT 'pg_catalog.pg_namespace'::pg_catalog.regclass::pg_catalog.oid AS class_oid,n.oid AS object_oid,n.oid AS namespace_oid,n.nspname::pg_catalog.text AS namespace_name,n.nspname::pg_catalog.text AS name,'namespace'::pg_catalog.text AS kind,n.nspowner AS owner_oid
 FROM pg_catalog.pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
 UNION ALL
 SELECT 'pg_catalog.pg_class'::pg_catalog.regclass::pg_catalog.oid,c.oid,n.oid,n.nspname::pg_catalog.text,c.relname::pg_catalog.text,c.relkind::pg_catalog.text,c.relowner FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
 UNION ALL
 SELECT 'pg_catalog.pg_type'::pg_catalog.regclass::pg_catalog.oid,t.oid,n.oid,n.nspname::pg_catalog.text,t.typname::pg_catalog.text,'type',t.typowner FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
 UNION ALL
 SELECT 'pg_catalog.pg_proc'::pg_catalog.regclass::pg_catalog.oid,p.oid,n.oid,n.nspname::pg_catalog.text,p.proname::pg_catalog.text,'function',p.proowner FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
 UNION ALL
 SELECT 'pg_catalog.pg_constraint'::pg_catalog.regclass::pg_catalog.oid,c.oid,n.oid,n.nspname::pg_catalog.text,c.conname::pg_catalog.text,'constraint',COALESCE(r.relowner,t.typowner) FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_namespace n ON n.oid=c.connamespace LEFT JOIN pg_catalog.pg_class r ON r.oid=c.conrelid LEFT JOIN pg_catalog.pg_type t ON t.oid=c.contypid WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
 UNION ALL
 SELECT 'pg_catalog.pg_trigger'::pg_catalog.regclass::pg_catalog.oid,t.oid,n.oid,n.nspname::pg_catalog.text,t.tgname::pg_catalog.text,'trigger',r.relowner FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class r ON r.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
 UNION ALL
 SELECT 'pg_catalog.pg_rewrite'::pg_catalog.regclass::pg_catalog.oid,r.oid,n.oid,n.nspname::pg_catalog.text,r.rulename::pg_catalog.text,'rule',c.relowner FROM pg_catalog.pg_rewrite r JOIN pg_catalog.pg_class c ON c.oid=r.ev_class JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
 UNION ALL
 SELECT 'pg_catalog.pg_collation'::pg_catalog.regclass::pg_catalog.oid,c.oid,n.oid,n.nspname::pg_catalog.text,c.collname::pg_catalog.text,'collation',c.collowner FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
 UNION ALL
 SELECT 'pg_catalog.pg_extension'::pg_catalog.regclass::pg_catalog.oid,e.oid,NULL::pg_catalog.oid,NULL::pg_catalog.text,e.extname::pg_catalog.text,'extension',e.extowner FROM pg_catalog.pg_extension e WHERE e.extname<>'plpgsql'
 UNION ALL
 SELECT 'pg_catalog.pg_statistic_ext'::pg_catalog.regclass::pg_catalog.oid,s.oid,n.oid,n.nspname::pg_catalog.text,s.stxname::pg_catalog.text,'statistics',s.stxowner FROM pg_catalog.pg_statistic_ext s JOIN pg_catalog.pg_namespace n ON n.oid=s.stxnamespace WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
)
SELECT class_oid,object_oid,namespace_oid,namespace_name,name,kind,owner_oid FROM known
UNION ALL
SELECT d.classid,d.objid,n.oid,i.schema,COALESCE(i.name,i.identity),i.type,d.refobjid
FROM pg_catalog.pg_shdepend d
CROSS JOIN LATERAL pg_catalog.pg_identify_object(d.classid,d.objid,d.objsubid) i
LEFT JOIN pg_catalog.pg_namespace n ON n.nspname=i.schema
WHERE d.dbid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
 AND d.deptype='o' AND d.objid>=16384
 AND d.classid<>'pg_catalog.pg_largeobject'::pg_catalog.regclass
 AND NOT EXISTS (SELECT 1 FROM known k WHERE k.class_oid=d.classid AND k.object_oid=d.objid)
ORDER BY class_oid,object_oid LIMIT 65537
"#;

pub(crate) fn clone_object_inventory(
    connection: &mut PgConnection,
    io: &IoControl,
) -> Result<Vec<OwnedObject>> {
    let result = connection.query(OBJECTS_SQL, io)?;
    if result.rows() > MAX_OBJECTS || result.columns()? != 7 {
        return Err(refusal(
            "clone object inventory exceeds its bound or has an invalid shape",
        ));
    }
    let mut remaining = CATALOG_BYTES;
    let mut objects = Vec::with_capacity(result.rows());
    for row in 0..result.rows() {
        let namespace_oid = if result.cell(row, 2)?.is_some() {
            Some(cdc_object_id(text(&result, row, 2)?)?)
        } else {
            None
        };
        let namespace_name = if result.cell(row, 3)?.is_some() {
            Some(text(&result, row, 3)?.to_owned())
        } else {
            None
        };
        let object = OwnedObject {
            class_oid: cdc_object_id(text(&result, row, 0)?)?,
            object_oid: cdc_object_id(text(&result, row, 1)?)?,
            object_sub_id: 0,
            namespace_oid,
            namespace_name,
            name: text(&result, row, 4)?.into(),
            kind: text(&result, row, 5)?.into(),
            owner_oid: cdc_object_id(text(&result, row, 6)?)?,
        };
        charge(
            &mut remaining,
            std::mem::size_of::<OwnedObject>()
                + object.name.len()
                + object.kind.len()
                + object.namespace_name.as_ref().map_or(0, String::len),
        )?;
        objects.push(object);
    }
    Ok(objects)
}

pub(crate) fn verify_exact_exclusions(
    actual: &[OwnedObject],
    excluded: &[OwnedObject],
) -> Result<()> {
    let mut keys = BTreeSet::new();
    for object in excluded {
        if object.object_oid == 0
            || object.class_oid == 0
            || object.object_sub_id != 0
            || !keys.insert((object.class_oid, object.object_oid))
            || actual.iter().filter(|item| *item == object).count() != 1
        {
            return Err(refusal(
                "clone bookkeeping exclusion lacks exact current object provenance",
            ));
        }
    }
    Ok(())
}

fn require_source_obligations(extension_configuration: Option<&[u8]>) -> Result<()> {
    if extension_configuration != Some(b"f") {
        return Err(refusal(
            "clone does not support extension configuration data",
        ));
    }
    Ok(())
}

fn source_obligations(connection: &mut PgConnection, io: &IoControl) -> Result<()> {
    let result = connection.query("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE COALESCE(pg_catalog.cardinality(extconfig),0)>0)", io)?;
    if result.rows() != 1 || result.columns()? != 1 {
        return Err(refusal(
            "clone extension configuration inventory is absent or invalid",
        ));
    }
    require_source_obligations(result.cell(0, 0)?)
}

fn clone_columns(
    connection: &mut PgConnection,
    oid: u32,
    io: &IoControl,
) -> Result<(Vec<String>, Vec<String>)> {
    let result=connection.query(&format!(r#"WITH RECURSIVE roots AS (
 SELECT a.attnum,a.attname,a.attgenerated,a.atttypid FROM pg_catalog.pg_attribute a WHERE a.attrelid={oid} AND a.attnum>0 AND NOT a.attisdropped
), types AS (
 SELECT attnum,atttypid AS oid,0 AS depth FROM roots
 UNION ALL
 SELECT x.attnum,CASE WHEN t.typtype='d' THEN t.typbasetype WHEN t.typelem<>0 AND t.typlen=-1 THEN t.typelem ELSE 0 END,x.depth+1
 FROM types x JOIN pg_catalog.pg_type t ON t.oid=x.oid WHERE x.depth<32 AND (t.typtype='d' OR (t.typelem<>0 AND t.typlen=-1))
)
SELECT r.attname,r.attgenerated,COALESCE(pg_catalog.bool_and(
 x.depth<32 AND (t.typtype IN ('d','e') OR (t.typelem<>0 AND t.typlen=-1) OR
 (n.nspname='pg_catalog' AND t.typtype IN ('b','r','m') AND t.oid<16384 AND t.typname NOT IN ('regclass','regcollation','regconfig','regdictionary','regnamespace','regoper','regoperator','regproc','regprocedure','regrole','regtype','aclitem','pg_node_tree')))),false)
FROM roots r JOIN types x USING(attnum) LEFT JOIN pg_catalog.pg_type t ON t.oid=x.oid LEFT JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
GROUP BY r.attnum,r.attname,r.attgenerated ORDER BY r.attnum LIMIT 1601"#),io)?;
    if result.rows() > 1600 || result.columns()? != 3 {
        return Err(refusal("clone requires a bounded supported column layout"));
    }
    let mut writable = Vec::new();
    let mut all = Vec::new();
    for row in 0..result.rows() {
        if result.cell(row, 2)? != Some(b"t") {
            return Err(refusal(
                "clone column type has an unsupported textual representation",
            ));
        }
        let name = text(&result, row, 0)?.to_owned();
        if text(&result, row, 1)?.is_empty() {
            writable.push(name.clone());
        }
        all.push(name);
    }
    if writable.is_empty() {
        return Err(refusal(
            "clone requires at least one writable column per heap",
        ));
    }
    Ok((writable, all))
}

pub(super) fn clone_identity_modes(
    connection: &mut PgConnection,
    oid: u32,
    columns: &[String],
    io: &IoControl,
) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
    let result = connection.query(&format!(r#"SELECT c.relispartition,a.attname,a.attidentity,r.attname,r.attidentity
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_attribute r ON c.relispartition AND r.attrelid=pg_catalog.pg_partition_root(c.oid) AND r.attname=a.attname AND r.attnum>0 AND NOT r.attisdropped
WHERE c.oid={oid} AND c.relkind='r' ORDER BY a.attnum LIMIT 1601"#),io)?;
    if columns.is_empty()
        || columns.len() > 1600
        || result.rows() != columns.len()
        || result.columns()? != 5
    {
        return Err(refusal(
            "clone identity metadata has an incomplete or unbounded column layout",
        ));
    }
    let partition = match text(&result, 0, 0)? {
        "t" => true,
        "f" => false,
        _ => {
            return Err(refusal(
                "clone identity metadata lacks declarative partition evidence",
            ));
        }
    };
    let mut local = Vec::with_capacity(columns.len());
    let mut root = partition.then(|| Vec::with_capacity(columns.len()));
    let mut names = BTreeSet::new();
    for (row, name) in columns.iter().enumerate() {
        if text(&result, row, 0)? != if partition { "t" } else { "f" }
            || text(&result, row, 1)? != name
            || !names.insert(name)
        {
            return Err(refusal(
                "clone identity metadata column names differ from the plan",
            ));
        }
        local.push(identity_mode(text(&result, row, 2)?)?);
        if let Some(root) = &mut root {
            if text(&result, row, 3)? != name {
                return Err(refusal(
                    "clone partition identity metadata lacks an exact root column",
                ));
            }
            root.push(identity_mode(text(&result, row, 4)?)?);
        } else if result.cell(row, 3)?.is_some() || result.cell(row, 4)?.is_some() {
            return Err(refusal(
                "clone ordinary table unexpectedly has partition identity metadata",
            ));
        }
    }
    Ok((local, root))
}

fn identity_mode(value: &str) -> Result<u8> {
    match value {
        "" => Ok(0),
        "a" => Ok(b'a'),
        "d" => Ok(b'd'),
        _ => Err(refusal(
            "clone identity metadata has an invalid generation mode",
        )),
    }
}

pub(super) fn require_identity_compatibility(
    source_major: u32,
    target_major: u32,
    tables: &[CloneTable],
) -> Result<()> {
    if source_major == 16
        && target_major == 18
        && tables.iter().any(|table| {
            table.partition_identity_modes.is_some()
                && table.identity_modes.iter().any(|mode| *mode != 0)
        })
    {
        return Err(refusal(
            "clone identity: independent PostgreSQL 16 leaf identity cannot be represented on PostgreSQL 18",
        ));
    }
    Ok(())
}

pub(super) fn require_target_identity_modes(
    table: &CloneTable,
    target_major: u32,
    local: &[u8],
    root: Option<&[u8]>,
) -> Result<()> {
    let expected_local = if target_major == 18 && table.identity_modes.iter().all(|mode| *mode == 0)
    {
        table
            .partition_identity_modes
            .as_deref()
            .unwrap_or(&table.identity_modes)
    } else {
        &table.identity_modes
    };
    if local != expected_local || root != table.partition_identity_modes.as_deref() {
        return Err(refusal(
            "clone target identity generation modes differ from the source plan",
        ));
    }
    Ok(())
}

pub(crate) fn inspect_clone_tables(
    connection: &mut PgConnection,
    exact_exclusions: &[OwnedObject],
    io: &IoControl,
) -> Result<Vec<CloneTable>> {
    let inventory = clone_object_inventory(connection, io)?;
    verify_exact_exclusions(&inventory, exact_exclusions)?;
    source_obligations(connection, io)?;
    let mut tables = Vec::new();
    let mut after = 0;
    let mut budget = CATALOG_BYTES;
    loop {
        let result=connection.query(&format!("SELECT c.oid,n.nspname,c.relname,c.relkind,c.relpersistence,COALESCE(am.amname,''), EXISTS(SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass AND d.objid=c.oid AND d.deptype='e'), EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid AND t.tgenabled IN ('A','R')) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_catalog.pg_am am ON am.oid=c.relam WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND c.relkind IN ('r','p','v','m','f','S') AND c.oid>{after} ORDER BY c.oid LIMIT {CATALOG_PAGE}"),io)?;
        for row in 0..result.rows() {
            let oid = cdc_object_id(text(&result, row, 0)?)?;
            if oid <= after {
                return Err(refusal("clone relation inventory did not advance"));
            }
            after = oid;
            if exact_exclusions
                .iter()
                .any(|o| o.class_oid == 1259 && o.object_oid == oid)
            {
                continue;
            }
            let kind = text(&result, row, 3)?;
            if !admitted_relation(
                kind,
                text(&result, row, 4)?,
                text(&result, row, 5)?,
                result.cell(row, 6)? == Some(b"t"),
                result.cell(row, 7)? == Some(b"t"),
            ) {
                return Err(refusal(
                    "clone refuses foreign, materialized, unlogged, extension-owned, custom-access-method, or ALWAYS/REPLICA-trigger relations",
                ));
            }
            if kind != "r" {
                continue;
            }
            if tables.len() >= MAX_TABLES {
                return Err(refusal("clone exceeds the 4096 physical table bound"));
            }
            let name = QualifiedTable {
                schema: text(&result, row, 1)?.into(),
                table: text(&result, row, 2)?.into(),
            };
            require_copy_name(&name)?;
            let quoted = qualified(connection, &name.schema, &name.table)?;
            command(
                connection,
                &format!("LOCK TABLE ONLY {quoted} IN ACCESS SHARE MODE"),
                "LOCK TABLE",
                io,
            )?;
            let (writable_columns, all_columns) = clone_columns(connection, oid, io)?;
            let (identity_modes, partition_identity_modes) =
                clone_identity_modes(connection, oid, &all_columns, io)?;
            let selected = writable_columns
                .iter()
                .map(|name| connection.quote_identifier(name))
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            connection.query(&format!("SELECT {selected} FROM ONLY {quoted} LIMIT 0"), io)?;
            charge(
                &mut budget,
                std::mem::size_of::<CloneTable>()
                    + name.schema.len()
                    + name.table.len()
                    + all_columns.iter().map(String::len).sum::<usize>()
                    + writable_columns.iter().map(String::len).sum::<usize>()
                    + identity_modes.capacity()
                    + partition_identity_modes.as_ref().map_or(0, Vec::capacity),
            )?;
            tables.push(CloneTable {
                source_oid: oid,
                name,
                writable_columns,
                all_columns,
                identity_modes,
                partition_identity_modes,
            });
        }
        if result.rows() < CATALOG_PAGE {
            break;
        }
    }
    tables.sort_by(|a, b| (&a.name.schema, &a.name.table).cmp(&(&b.name.schema, &b.name.table)));
    Ok(tables)
}

/// The caller keeps source and target DDL quiescent through the ownership handoff.
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_clone(
    source: &mut PgConnection,
    target: &mut PgConnection,
    generation: CopyGeneration,
    policy: RestorePolicy,
    copy_policy: CloneCopyPolicy,
    exact_metadata: &[OwnedObject],
    owned_objects: &[OwnedObject],
    io: &IoControl,
) -> Result<ClonePlan> {
    configure_session(source, io)?;
    configure_session(target, io)?;
    let source_id = read_clone_source(source, io)?;
    let target_id = read_clone_target(target, io)?;
    if generation.0 == 0
        || source_id.major > target_id.major
        || source_id.identity == target_id.identity
    {
        return Err(refusal(
            "clone requires distinct source/target identities and an admitted 16/18 forward version pair",
        ));
    }
    let tables = inspect_clone_tables(source, &[], io)?;
    require_identity_compatibility(source_id.major, target_id.major, &tables)?;
    source_obligations(target, io)?;
    let objects = clone_object_inventory(target, io)?;
    verify_exact_exclusions(&objects, exact_metadata)?;
    verify_exact_exclusions(&objects, owned_objects)?;
    if objects.iter().any(|object| {
        !exact_metadata.contains(object)
            && !owned_objects.contains(object)
            && !(object.class_oid == 2615 && object.name == "public" && object.kind == "namespace")
    }) {
        return Err(refusal(
            "clone requires a collision-free target; existing objects are never adopted",
        ));
    }
    let source_objects = clone_object_inventory(source, io)?;
    if exact_metadata
        .iter()
        .filter_map(|o| o.namespace_name.as_deref())
        .any(|name| {
            source_objects
                .iter()
                .any(|o| o.namespace_name.as_deref() == Some(name))
        })
    {
        return Err(refusal(
            "source schema collides with exact target bookkeeping namespace",
        ));
    }
    let sequences = source_objects
        .iter()
        .filter(|o| o.class_oid == 1259 && o.kind == "S")
        .map(|o| {
            Ok(QualifiedTable {
                schema: o
                    .namespace_name
                    .clone()
                    .ok_or_else(|| refusal("sequence namespace is absent"))?,
                table: o.name.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ClonePlan {
        source: source_id,
        target: target_id,
        generation,
        copy_tables: provisional_copy_tables(&tables)?,
        tables,
        sequences,
        policy,
        copy_policy,
    })
}

pub(crate) fn reconcile_clone_tables(
    target: &mut PgConnection,
    plan: &ClonePlan,
    objects: &[OwnedObject],
    io: &IoControl,
) -> Result<Vec<OwnedCopyTable>> {
    if read_clone_target(target, io)? != plan.target {
        return Err(refusal(
            "clone target identity changed before ownership reconciliation",
        ));
    }
    let actual = clone_object_inventory(target, io)?;
    verify_exact_exclusions(&actual, objects)?;
    let mut tables = Vec::new();
    for table in &plan.tables {
        let matches = objects
            .iter()
            .filter(|o| {
                o.class_oid == 1259
                    && o.kind == "r"
                    && o.namespace_name.as_deref() == Some(&table.name.schema)
                    && o.name == table.name.table
            })
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err(refusal(
                "pre-data handoff lacks one exact newly created target heap",
            ));
        }
        let object = matches[0];
        let (writable, all) = clone_columns(target, object.object_oid, io)?;
        if writable != table.writable_columns || all != table.all_columns {
            return Err(refusal(
                "pre-data target column layout differs from the source plan",
            ));
        }
        let (local, root) = clone_identity_modes(target, object.object_oid, &all, io)?;
        require_target_identity_modes(table, plan.target.major, &local, root.as_deref())?;
        tables.push(OwnedCopyTable {
            table: table.clone(),
            target_oid: object.object_oid,
            target_namespace_oid: object
                .namespace_oid
                .ok_or_else(|| refusal("target heap namespace identity is absent"))?,
            target_owner_oid: object.owner_oid,
            generation: plan.generation,
        });
    }
    Ok(tables)
}

pub(crate) fn sync_clone_sequences(
    source: &mut PgConnection,
    target: &mut PgConnection,
    plan: &ClonePlan,
    owned: &[OwnedObject],
    io: &IoControl,
) -> Result<()> {
    if read_clone_source(source, io)? != plan.source
        || read_clone_target(target, io)? != plan.target
    {
        return Err(refusal(
            "sequence synchronization database identity changed",
        ));
    }
    let actual = clone_object_inventory(target, io)?;
    verify_exact_exclusions(&actual, owned)?;
    for name in &plan.sequences {
        if owned
            .iter()
            .filter(|o| {
                o.class_oid == 1259
                    && o.kind == "S"
                    && o.namespace_name.as_deref() == Some(&name.schema)
                    && o.name == name.table
            })
            .count()
            != 1
        {
            return Err(refusal(
                "sequence mutation requires an exact newly created owned target sequence",
            ));
        }
        let src = qualified(source, &name.schema, &name.table)?;
        let dst = qualified(target, &name.schema, &name.table)?;
        let state = source.query(&format!("SELECT last_value,is_called FROM {src}"), io)?;
        if state.rows() != 1 {
            return Err(refusal("sequence read returned an invalid shape"));
        }
        let value = text(&state, 0, 0)?
            .parse::<i64>()
            .map_err(|_| refusal("sequence returned an invalid value"))?;
        let called = match text(&state, 0, 1)? {
            "t" => "true",
            "f" => "false",
            _ => return Err(refusal("sequence returned an invalid state")),
        };
        let literal = target.quote_literal(&dst)?;
        let result = target.query(
            &format!("SELECT pg_catalog.setval({literal}::pg_catalog.regclass,{value},{called})"),
            io,
        )?;
        if result.rows() != 1 || text(&result, 0, 0)? != value.to_string() {
            return Err(refusal("sequence synchronization has no positive result"));
        }
    }
    Ok(())
}

pub(crate) fn analyze_clone(
    target: &mut PgConnection,
    plan: &ClonePlan,
    owned: &[OwnedCopyTable],
    io: &IoControl,
) -> Result<()> {
    if read_clone_target(target, io)? != plan.target {
        return Err(refusal("ANALYZE target identity changed"));
    }
    let actual = clone_object_inventory(target, io)?;
    if owned.len() != plan.tables.len() {
        return Err(refusal(
            "ANALYZE requires complete owned target table inventory",
        ));
    }
    for table in &plan.tables {
        let matches = owned
            .iter()
            .filter(|o| o.table == *table && o.generation == plan.generation)
            .collect::<Vec<_>>();
        if matches.len() != 1
            || !actual.iter().any(|o| {
                o.class_oid == 1259
                    && o.object_oid == matches[0].target_oid
                    && o.namespace_oid == Some(matches[0].target_namespace_oid)
                    && o.owner_oid == matches[0].target_owner_oid
                    && o.namespace_name.as_deref() == Some(&table.name.schema)
                    && o.name == table.name.table
                    && o.kind == "r"
            })
        {
            return Err(refusal("ANALYZE target lacks exact owned heap identity"));
        }
        let name = qualified(target, &table.name.schema, &table.name.table)?;
        command(target, &format!("ANALYZE {name}"), "ANALYZE", io)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provisional_copy_profiles_are_sorted_exact_and_bounded() {
        let table = |oid| CloneTable {
            source_oid: oid,
            name: QualifiedTable {
                schema: "public".into(),
                table: format!("items_{oid}"),
            },
            writable_columns: vec!["id".into()],
            all_columns: vec!["id".into()],
            identity_modes: vec![0],
            partition_identity_modes: None,
        };
        let actual = provisional_copy_tables(&[table(9), table(3)]).unwrap();
        assert_eq!(
            actual
                .iter()
                .map(|item| item.source_oid)
                .collect::<Vec<_>>(),
            [3, 9]
        );
        for item in &actual {
            assert_eq!(item.format, CopyFormat::Text);
            assert_eq!(
                item.parts,
                [CopyPart {
                    number: 1,
                    count: 1,
                    start_block: 0,
                    end_block: None
                }]
            );
        }
        assert!(provisional_copy_tables(&[table(0)]).is_err());
        assert!(provisional_copy_tables(&[table(3), table(3)]).is_err());
        let at_bound = (1..=4096).map(table).collect::<Vec<_>>();
        assert_eq!(provisional_copy_tables(&at_bound).unwrap().len(), 4096);
        let over_bound = (1..=4097).map(table).collect::<Vec<_>>();
        assert!(provisional_copy_tables(&over_bound).is_err());
        assert!(provisional_copy_tables(&[]).unwrap().is_empty());
    }

    #[test]
    fn extension_configuration_remains_refused_without_coupling_large_objects() {
        require_source_obligations(Some(b"f")).unwrap();
        for flag in [
            None,
            Some(b"t".as_slice()),
            Some(b"".as_slice()),
            Some(b"false".as_slice()),
        ] {
            assert!(require_source_obligations(flag).is_err());
        }
    }

    #[test]
    fn copy_parts_use_physical_blocks_with_an_unbounded_tail() {
        let singleton = vec![(1, 1, 0, None)];
        for (bytes, threshold, maximum, expected) in [
            (0, 8192, 8, singleton.clone()),
            (8192, 0, 8, singleton.clone()),
            (8192, 8192, 8, singleton.clone()),
            (16384, 8192, 1, singleton.clone()),
            (16384, 8192, 8, vec![(1, 2, 0, Some(1)), (2, 2, 1, None)]),
            (
                24577,
                8192,
                8,
                vec![
                    (1, 4, 0, Some(1)),
                    (2, 4, 1, Some(2)),
                    (3, 4, 2, Some(3)),
                    (4, 4, 3, None),
                ],
            ),
            (
                81920,
                8192,
                3,
                vec![(1, 3, 0, Some(4)), (2, 3, 4, Some(8)), (3, 3, 8, None)],
            ),
        ] {
            let actual = plan_copy_parts(bytes, 8192, threshold, maximum).unwrap();
            assert_eq!(
                actual
                    .iter()
                    .map(|p| (p.number, p.count, p.start_block, p.end_block))
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn copy_parts_refuse_invalid_sizes_and_excess_actual_work_before_allocation() {
        for (bytes, block, threshold, maximum) in [
            (8192, 0, 1, 8),
            (8192, 8192, 1, 0),
            (8192, 8192, 1, u32::MAX),
            (u64::MAX, 8192, 1, 8),
            ((u64::from(u32::MAX) + 1) * 8192, 8192, 1, 8),
            (8192 * 4097, 8192, 8192, 4097),
        ] {
            assert!(plan_copy_parts(bytes, block, threshold, maximum).is_err());
        }
        assert_eq!(
            plan_copy_parts(1, 8192, 1, i32::MAX as u32).unwrap().len(),
            1
        );
    }

    fn binary_fixture(oid: u32, name: &str, send_oid: u32, receive_oid: u32) -> BinaryColumn {
        let io_name = if matches!(
            name,
            "numeric"
                | "date"
                | "time"
                | "timestamp"
                | "timestamptz"
                | "interval"
                | "uuid"
                | "json"
                | "jsonb"
        ) {
            format!("{name}_")
        } else {
            name.into()
        };
        BinaryColumn {
            name: "value".into(),
            type_oid: oid,
            namespace_oid: 11,
            type_name: name.into(),
            kind: "b".into(),
            modifier: -1,
            scalar: true,
            send_oid,
            receive_oid,
            send_name: format!("{io_name}send"),
            receive_name: format!("{io_name}recv"),
            send_builtin: true,
            receive_builtin: true,
            extension_owned: false,
        }
    }

    #[test]
    fn binary_columns_admit_only_exact_builtin_scalar_profiles() {
        for (oid, name, send, receive) in [
            (16, "bool", 2437, 2436),
            (17, "bytea", 2413, 2412),
            (21, "int2", 2405, 2404),
            (23, "int4", 2407, 2406),
            (20, "int8", 2409, 2408),
            (700, "float4", 2425, 2424),
            (701, "float8", 2427, 2426),
            (1700, "numeric", 2461, 2460),
            (25, "text", 2415, 2414),
            (1043, "varchar", 2433, 2432),
            (1042, "bpchar", 2431, 2430),
            (1082, "date", 2469, 2468),
            (1083, "time", 2471, 2470),
            (1114, "timestamp", 2475, 2474),
            (1184, "timestamptz", 2477, 2476),
            (1186, "interval", 2479, 2478),
            (2950, "uuid", 2962, 2961),
            (114, "json", 324, 323),
            (3802, "jsonb", 3803, 3805),
        ] {
            let column = binary_fixture(oid, name, send, receive);
            assert!(binary_columns_compatible(
                std::slice::from_ref(&column),
                std::slice::from_ref(&column),
                ("UTF8", "UTF8"),
                ("UTF8", "UTF8")
            ));
        }
    }

    #[test]
    fn binary_columns_fall_back_for_spoofs_containers_layout_and_encoding_drift() {
        let original = binary_fixture(23, "int4", 2407, 2406);
        let compatible = |s: &BinaryColumn, t: &BinaryColumn| {
            binary_columns_compatible(
                std::slice::from_ref(s),
                std::slice::from_ref(t),
                ("UTF8", "UTF8"),
                ("UTF8", "UTF8"),
            )
        };
        for mutate in [
            |c: &mut BinaryColumn| c.type_oid = 17001,
            |c: &mut BinaryColumn| c.namespace_oid = 17001,
            |c: &mut BinaryColumn| c.type_name = "tsvector".into(),
            |c: &mut BinaryColumn| c.scalar = false,
            |c: &mut BinaryColumn| c.send_name = "evil".into(),
            |c: &mut BinaryColumn| c.receive_name = "evil".into(),
            |c: &mut BinaryColumn| c.send_oid = 0,
            |c: &mut BinaryColumn| c.receive_oid = 0,
            |c: &mut BinaryColumn| c.send_builtin = false,
            |c: &mut BinaryColumn| c.receive_builtin = false,
            |c: &mut BinaryColumn| c.extension_owned = true,
        ] {
            let mut changed = original.clone();
            mutate(&mut changed);
            assert!(!compatible(&changed, &changed));
            assert!(!compatible(&original, &changed));
        }
        for kind in ["d", "e", "c", "r", "m", "p"] {
            let mut changed = original.clone();
            changed.kind = kind.into();
            assert!(!compatible(&changed, &changed));
        }
        let mut target = original.clone();
        target.modifier = 14;
        assert!(!compatible(&original, &target));
        target = original.clone();
        target.name = "other".into();
        assert!(!compatible(&original, &target));
        assert!(!binary_columns_compatible(
            &[],
            &[],
            ("UTF8", "UTF8"),
            ("UTF8", "UTF8")
        ));
        for encoding in [("LATIN1", "UTF8"), ("UTF8", "LATIN1"), ("", "")] {
            assert!(!binary_columns_compatible(
                std::slice::from_ref(&original),
                std::slice::from_ref(&original),
                ("UTF8", "UTF8"),
                encoding
            ));
        }
        assert!(!binary_columns_compatible(
            std::slice::from_ref(&original),
            &[],
            ("UTF8", "UTF8"),
            ("UTF8", "UTF8")
        ));
    }

    #[test]
    fn target_modes_normalize_only_zero_local_declarative_leaves_on_pg18() {
        let mut table = CloneTable {
            source_oid: 42,
            name: QualifiedTable {
                schema: "public".into(),
                table: "leaf".into(),
            },
            all_columns: vec!["id".into(), "body".into()],
            writable_columns: vec!["id".into(), "body".into()],
            identity_modes: vec![0, 0],
            partition_identity_modes: Some(vec![b'a', 0]),
        };
        require_target_identity_modes(&table, 16, &[0, 0], Some(&[b'a', 0])).unwrap();
        require_target_identity_modes(&table, 18, &[b'a', 0], Some(&[b'a', 0])).unwrap();
        assert!(require_target_identity_modes(&table, 18, &[0, 0], Some(&[b'a', 0])).is_err());
        assert!(require_target_identity_modes(&table, 18, &[b'd', 0], Some(&[b'a', 0])).is_err());
        assert!(require_target_identity_modes(&table, 18, &[b'a', 0], Some(&[b'd', 0])).is_err());
        assert!(require_target_identity_modes(&table, 16, &[b'a', 0], Some(&[b'a', 0])).is_err());
        table.identity_modes[0] = b'd';
        require_target_identity_modes(&table, 16, &[b'd', 0], Some(&[b'a', 0])).unwrap();
        require_identity_compatibility(16, 16, std::slice::from_ref(&table)).unwrap();
        assert!(
            require_identity_compatibility(16, 18, std::slice::from_ref(&table))
                .unwrap_err()
                .message
                .contains("independent")
        );
        table.partition_identity_modes = None;
        require_identity_compatibility(16, 18, std::slice::from_ref(&table)).unwrap();
        require_target_identity_modes(&table, 18, &[b'd', 0], None).unwrap();
        assert!(require_target_identity_modes(&table, 18, &[b'a', 0], None).is_err());
    }

    #[test]
    fn identity_mode_bytes_require_exact_catalog_values() {
        for (value, expected) in [("", 0), ("a", b'a'), ("d", b'd')] {
            assert_eq!(identity_mode(value).unwrap(), expected);
        }
        for invalid in ["0", "A", "ad", " "] {
            assert!(identity_mode(invalid).is_err());
        }
    }

    #[test]
    #[ignore = "requires owned PostgreSQL harness"]
    fn live_identity_catalog_tracks_physical_and_partition_root_modes() {
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
        let schema = format!("identity_catalog_{}", std::process::id());
        for sql in [
            format!("CREATE SCHEMA {schema}"),
            format!(
                "CREATE TABLE {schema}.plain(id integer GENERATED ALWAYS AS IDENTITY, second integer GENERATED BY DEFAULT AS IDENTITY)"
            ),
            format!(
                "CREATE TABLE {schema}.parent(id integer GENERATED ALWAYS AS IDENTITY, body integer)"
            ),
            format!("CREATE TABLE {schema}.child() INHERITS ({schema}.parent)"),
            format!(
                "ALTER TABLE {schema}.child ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY"
            ),
            format!(
                "CREATE TABLE {schema}.root(discarded integer,id integer GENERATED ALWAYS AS IDENTITY,body integer) PARTITION BY RANGE (body)"
            ),
            format!("ALTER TABLE {schema}.root DROP COLUMN discarded"),
            format!(
                "CREATE TABLE {schema}.leaf PARTITION OF {schema}.root FOR VALUES FROM (0) TO (100)"
            ),
        ] {
            connection.query(&sql, &io).unwrap();
        }
        let tables = inspect_clone_tables(&mut connection, &[], &io).unwrap();
        let get = |name: &str| {
            tables
                .iter()
                .find(|t| t.name.schema == schema && t.name.table == name)
                .unwrap()
        };
        assert_eq!(get("plain").identity_modes, [b'a', b'd']);
        assert_eq!(get("plain").partition_identity_modes, None);
        assert_eq!(get("parent").identity_modes, [b'a', 0]);
        assert_eq!(get("child").identity_modes, [b'd', 0]);
        assert_eq!(get("child").partition_identity_modes, None);
        assert_eq!(get("leaf").all_columns, ["id", "body"]);
        assert_eq!(
            get("leaf").identity_modes,
            if connection.server_version().unwrap() / 10_000 == 18 {
                vec![b'a', 0]
            } else {
                vec![0, 0]
            }
        );
        assert_eq!(get("leaf").partition_identity_modes, Some(vec![b'a', 0]));
        let oid = get("leaf").source_oid;
        assert!(
            clone_identity_modes(&mut connection, oid, &["body".into(), "id".into()], &io).is_err()
        );
        assert!(clone_identity_modes(&mut connection, oid, &["id".into()], &io).is_err());
        assert!(clone_identity_modes(&mut connection, oid, &[], &io).is_err());
        assert!(clone_identity_modes(&mut connection, 0, &["id".into()], &io).is_err());
        command(&mut connection, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    fn logged_heaps_and_partition_roots_admit_without_an_inheritance_whitelist() {
        assert!(admitted_relation("r", "p", "heap", false, false));
        assert!(admitted_relation("p", "p", "", false, false));
        assert!(admitted_relation("v", "p", "", false, false));
    }

    #[test]
    fn unsupported_storage_and_execution_states_refuse_before_mutation() {
        for (kind, persistence, am, extension, trigger) in [
            ("f", "p", "", false, false),
            ("m", "p", "heap", false, false),
            ("r", "u", "heap", false, false),
            ("r", "p", "custom", false, false),
            ("r", "p", "heap", true, false),
            ("r", "p", "heap", false, true),
        ] {
            assert!(!admitted_relation(
                kind,
                persistence,
                am,
                extension,
                trigger
            ));
        }
    }
    #[test]
    fn bookkeeping_exclusion_requires_every_recorded_identity_fact() {
        let object = OwnedObject {
            class_oid: 1259,
            object_oid: 20000,
            object_sub_id: 0,
            namespace_oid: Some(19000),
            namespace_name: Some("metadata".into()),
            name: "checkpoint".into(),
            kind: "r".into(),
            owner_oid: 10,
        };
        verify_exact_exclusions(std::slice::from_ref(&object), std::slice::from_ref(&object))
            .unwrap();
        for field in 0..8 {
            let mut changed = object.clone();
            match field {
                0 => changed.class_oid += 1,
                1 => changed.object_oid += 1,
                2 => changed.object_sub_id = 1,
                3 => changed.namespace_oid = None,
                4 => changed.namespace_name = Some("other".into()),
                5 => changed.name = "foreign".into(),
                6 => changed.kind = "i".into(),
                7 => changed.owner_oid += 1,
                _ => unreachable!(),
            }
            assert!(verify_exact_exclusions(std::slice::from_ref(&object), &[changed]).is_err());
        }
        assert!(verify_exact_exclusions(&[], std::slice::from_ref(&object)).is_err());
        assert!(
            verify_exact_exclusions(
                std::slice::from_ref(&object),
                &[object.clone(), object.clone()]
            )
            .is_err()
        );
    }
}
