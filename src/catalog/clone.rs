use super::*;
use crate::contracts::{
    ClonePlan, CloneSource, CloneTable, CloneTarget, CopyGeneration, OwnedCopyTable, OwnedObject,
    RestorePolicy,
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

fn clone_identity(connection: &mut PgConnection, io: &IoControl) -> Result<(SourceId, u32, u32)> {
    let major = connection.server_version()? / 10_000;
    if !matches!(major, 16 | 18) {
        return Err(refusal("clone requires PostgreSQL 16 or 18"));
    }
    let result = connection.query("SELECT c.system_identifier::pg_catalog.text, d.oid, k.timeline_id, pg_catalog.pg_is_in_recovery() FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_control_checkpoint() k CROSS JOIN pg_catalog.pg_database d WHERE d.datname OPERATOR(pg_catalog.=) pg_catalog.current_database()",io)?;
    if result.rows() != 1 || result.columns()? != 4 || text(&result, 0, 3)? != "f" {
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

fn source_obligations(connection: &mut PgConnection, io: &IoControl) -> Result<()> {
    let result=connection.query("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE COALESCE(pg_catalog.cardinality(extconfig),0)>0), EXISTS (SELECT 1 FROM pg_catalog.pg_largeobject_metadata)",io)?;
    if result.rows() != 1 || result.cell(0, 0)? != Some(b"f") || result.cell(0, 1)? != Some(b"f") {
        return Err(refusal(
            "clone does not support extension configuration data or large objects",
        ));
    }
    Ok(())
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
        let result=connection.query(&format!("SELECT c.oid,n.nspname,c.relname,c.relkind,c.relpersistence,COALESCE(am.amname,''), EXISTS(SELECT 1 FROM pg_catalog.pg_depend d WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass AND d.objid=c.oid AND d.deptype='e'), EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal AND t.tgenabled IN ('A','R')) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_catalog.pg_am am ON am.oid=c.relam WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' AND c.relkind IN ('r','p','v','m','f','S') AND c.oid>{after} ORDER BY c.oid LIMIT {CATALOG_PAGE}"),io)?;
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
                    + writable_columns.iter().map(String::len).sum::<usize>(),
            )?;
            tables.push(CloneTable {
                source_oid: oid,
                name,
                writable_columns,
                all_columns,
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
pub(crate) fn plan_clone(
    source: &mut PgConnection,
    target: &mut PgConnection,
    generation: CopyGeneration,
    policy: RestorePolicy,
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
        tables,
        sequences,
        policy,
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
