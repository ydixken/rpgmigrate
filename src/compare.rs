use std::collections::BTreeMap;
use std::io::Write;
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::catalog;
use crate::contracts::{
    CloneOptions, CloneTable, Error, ErrorKind, IoControl, OwnedObject, Result,
};
use crate::pg::{CopyBuffer, PgConnection};
use crate::state::{self, CloneInspection};

const MAX_ROW_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Serialize)]
pub(crate) struct SchemaComparison {
    matched: bool,
}

impl SchemaComparison {
    pub(crate) fn matched(&self) -> bool {
        self.matched
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct TableMeasurement {
    rowcount: u64,
    checksum: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct TableComparison {
    schema: String,
    name: String,
    source: Option<TableMeasurement>,
    target: Option<TableMeasurement>,
    status: &'static str,
}

impl TableComparison {
    pub(crate) fn matched(&self) -> bool {
        self.status == "match"
    }
}

pub(crate) fn schema(options: &CloneOptions, io: &IoControl) -> Result<SchemaComparison> {
    let options = &crate::contracts::connection_files::bind_compare(options)?;
    let (mut source, mut target, inspection, metadata) = peers(options, io)?;
    let mut effective = options.clone();
    effective.policy = inspection.clone_state.plan.policy;
    let matched = crate::schema::compare(&effective, &metadata, io)?;
    catalog::command(&mut source, "COMMIT", "COMMIT", io)?;
    catalog::command(&mut target, "COMMIT", "COMMIT", io)?;
    Ok(SchemaComparison { matched })
}

pub(crate) fn data(options: &CloneOptions, io: &IoControl) -> Result<Vec<TableComparison>> {
    let options = &crate::contracts::connection_files::bind_compare(options)?;
    let started = Instant::now();
    let (mut source, mut target, _inspection, metadata) = peers(options, io)?;
    let source_tables = catalog::inspect_clone_tables(&mut source, &[], io)?;
    let target_tables = catalog::inspect_clone_tables(&mut target, &metadata, io)?;
    let inventory = union_tables(&source_tables, &target_tables)?;
    let mut sizes = Vec::with_capacity(inventory.len());
    for (left, right) in &inventory {
        let size = match left {
            Some(table) => table_size(&mut source, table, io)?,
            None => table_size(
                &mut target,
                right.ok_or_else(|| refusal("comparison inventory is incomplete"))?,
                io,
            )?,
        };
        sizes.push(size);
    }
    let mut progress = CompareProgress::new(inventory.len(), &sizes, started)?;
    progress.emit(false, None)?;
    let mut reports = Vec::with_capacity(inventory.len());
    for ((left, right), size) in inventory.into_iter().zip(sizes) {
        let table = left
            .or(right)
            .ok_or_else(|| refusal("comparison inventory is incomplete"))?;
        if let Some(table) = left {
            start_table(&mut source, table, io)?;
        }
        if let Some(table) = right {
            start_table(&mut target, table, io)?;
        }
        let table_started = Instant::now();
        let mut emitted = Instant::now();
        let outcome = compare_streams(
            || {
                if left.is_some() {
                    source.read_copy_bounded(MAX_ROW_BYTES, io)
                } else {
                    Ok(None)
                }
            },
            || {
                if right.is_some() {
                    target.read_copy_bounded(MAX_ROW_BYTES, io)
                } else {
                    Ok(None)
                }
            },
            CopyBuffer::as_bytes,
            || {
                if emitted.elapsed() >= Duration::from_secs(10) {
                    progress.emit(false, Some((table, size, table_started)))?;
                    emitted = Instant::now();
                }
                Ok(())
            },
        );
        let (bytes_match, source_measurement, target_measurement) = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                progress.failed_tables += 1;
                progress.emit(true, None)?;
                return Err(error);
            }
        };
        let matched = bytes_match
            && matches!((left,right),(Some(left),Some(right)) if left.all_columns == right.all_columns);
        let rows = source_measurement.rows;
        reports.push(TableComparison {
            schema: table.name.schema.clone(),
            name: table.name.table.clone(),
            source: left.map(|_| source_measurement.into()),
            target: right.map(|_| target_measurement.into()),
            status: if matched { "match" } else { "different" },
        });
        progress.compared_tables += 1;
        progress.compared_bytes = checked_add(progress.compared_bytes, size)?;
        progress.compared_rows = checked_add(progress.compared_rows, rows)?;
        progress.differing_tables += u64::from(!matched);
        progress.emit(false, None)?;
    }
    catalog::command(&mut source, "COMMIT", "COMMIT", io)?;
    catalog::command(&mut target, "COMMIT", "COMMIT", io)?;
    progress.emit(true, None)?;
    Ok(reports)
}

pub(crate) fn peers(
    options: &CloneOptions,
    io: &IoControl,
) -> Result<(
    PgConnection,
    PgConnection,
    CloneInspection,
    Vec<OwnedObject>,
)> {
    let inspection = state::inspect_clone(&options.state, options.operator.as_ref(), io)?;
    let mut source = PgConnection::connect(&options.source, io)?;
    let mut target = PgConnection::connect(&options.target, io)?;
    for connection in [&mut source, &mut target] {
        catalog::command(
            connection,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            io,
        )?;
        catalog::configure_session(connection, io)?;
        catalog::command(
            connection,
            &format!(
                "SET application_name TO 'rpgmigrate[{}] compare'",
                std::process::id()
            ),
            "SET",
            io,
        )?;
    }
    if catalog::read_clone_source(&mut source, io)? != inspection.clone_state.plan.source
        || catalog::read_clone_target(&mut target, io)? != inspection.clone_state.plan.target
    {
        return Err(refusal(
            "comparison endpoints differ from the recorded clone identities",
        ));
    }
    let metadata =
        crate::apply::checkpoint::metadata_inventory(&mut target, &inspection.provenance, io)?;
    Ok((source, target, inspection, metadata))
}

fn union_tables<'a>(
    source: &'a [CloneTable],
    target: &'a [CloneTable],
) -> Result<Vec<(Option<&'a CloneTable>, Option<&'a CloneTable>)>> {
    let mut tables = BTreeMap::new();
    for (side, inventory) in [source, target].into_iter().enumerate() {
        for table in inventory {
            let entry = tables
                .entry((&table.name.schema, &table.name.table))
                .or_insert([None, None]);
            if entry[side].replace(table).is_some() {
                return Err(refusal(
                    "comparison inventory contains duplicate table identities",
                ));
            }
        }
    }
    if tables.is_empty() {
        return Err(refusal(
            "empty comparison inventory cannot establish legacy verification coverage",
        ));
    }
    Ok(tables
        .into_values()
        .map(|[source, target]| (source, target))
        .collect())
}

fn table_size(connection: &mut PgConnection, table: &CloneTable, io: &IoControl) -> Result<u64> {
    let result = connection.query(
        &format!(
            "SELECT pg_catalog.pg_table_size({}::pg_catalog.oid)",
            table.source_oid
        ),
        io,
    )?;
    if result.rows() != 1 || result.columns()? != 1 {
        return Err(refusal("comparison size measurement is absent"));
    }
    let value = result
        .cell(0, 0)?
        .ok_or_else(|| refusal("comparison size measurement is absent"))?;
    let count = std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|count| *count <= i64::MAX as u64)
        .ok_or_else(|| refusal("comparison size measurement is invalid"))?;
    Ok(count)
}

pub(crate) fn verify_generated_rows(
    source: &mut PgConnection,
    target: &mut PgConnection,
    table: &CloneTable,
    io: &IoControl,
) -> Result<()> {
    for connection in [&mut *source, &mut *target] {
        catalog::configure_session(connection, io)?;
    }
    start_table(source, table, io)?;
    start_table(target, table, io)?;
    let (matched, _, _) = compare_streams(
        || source.read_copy_bounded(MAX_ROW_BYTES, io),
        || target.read_copy_bounded(MAX_ROW_BYTES, io),
        CopyBuffer::as_bytes,
        || Ok(()),
    )?;
    if !matched {
        return Err(refusal(
            "clone-copy: generated values differ from the source snapshot",
        ));
    }
    Ok(())
}

fn start_table(connection: &mut PgConnection, table: &CloneTable, io: &IoControl) -> Result<()> {
    let qualified = catalog::qualified(connection, &table.name.schema, &table.name.table)?;
    let columns = table
        .all_columns
        .iter()
        .map(|column| connection.quote_identifier(column))
        .collect::<Result<Vec<_>>>()?;
    let order = if columns.is_empty() {
        String::new()
    } else {
        format!(
            " ORDER BY {}",
            columns
                .iter()
                .map(|column| format!(
                    "{column}::pg_catalog.text COLLATE pg_catalog.\"C\" NULLS FIRST"
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    connection.start_copy_out(
        &format!(
            "COPY (SELECT {} FROM ONLY {qualified}{order}) TO STDOUT (FORMAT text)",
            columns.join(", ")
        ),
        io,
    )
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CompareProgress {
    planned_tables: u64,
    compared_tables: u64,
    planned_bytes: u64,
    compared_bytes: u64,
    compared_rows: u64,
    differing_tables: u64,
    failed_tables: u64,
    #[serde(skip)]
    started: Instant,
}

impl CompareProgress {
    fn new(tables: usize, sizes: &[u64], started: Instant) -> Result<Self> {
        Ok(Self {
            planned_tables: tables as u64,
            compared_tables: 0,
            planned_bytes: sizes
                .iter()
                .try_fold(0, |sum, size| checked_add(sum, *size))?,
            compared_bytes: 0,
            compared_rows: 0,
            differing_tables: 0,
            failed_tables: 0,
            started,
        })
    }

    fn value(
        &self,
        final_result: bool,
        in_flight: Option<(&CloneTable, u64, Instant)>,
    ) -> Result<serde_json::Value> {
        let mut value = serde_json::to_value(self)
            .map_err(|_| refusal("cannot serialize comparison progress"))?;
        value["elapsedMs"] =
            serde_json::json!(self.started.elapsed().as_millis().min(i64::MAX as u128) as u64);
        value["final"] = serde_json::json!(final_result);
        value["inFlightCount"] = serde_json::json!(u64::from(in_flight.is_some()));
        value["inFlight"] = match in_flight {
            Some((table, bytes, started)) => {
                serde_json::json!([{"schema":table.name.schema,"name":table.name.table,"bytes":bytes,"elapsedMs":started.elapsed().as_millis().min(i64::MAX as u128) as u64}])
            }
            None => serde_json::json!([]),
        };
        Ok(value)
    }

    fn emit(
        &self,
        final_result: bool,
        in_flight: Option<(&CloneTable, u64, Instant)>,
    ) -> Result<()> {
        let value = self.value(final_result, in_flight)?;
        writeln!(std::io::stderr().lock(), "compare progress: {value}")
            .map_err(|_| refusal("cannot write comparison progress"))
    }
}

fn checked_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .filter(|count| *count <= i64::MAX as u64)
        .ok_or_else(|| refusal("comparison counter overflow"))
}

#[derive(Debug, Default, Eq, PartialEq)]
struct Measurement {
    rows: u64,
    bytes: u64,
    checksum: u32,
}

impl From<Measurement> for TableMeasurement {
    fn from(value: Measurement) -> Self {
        Self {
            rowcount: value.rows,
            checksum: format!("{:08x}", value.checksum),
        }
    }
}

fn compare_streams<B>(
    mut source: impl FnMut() -> Result<Option<B>>,
    mut target: impl FnMut() -> Result<Option<B>>,
    bytes: impl Fn(&B) -> &[u8],
    mut progress: impl FnMut() -> Result<()>,
) -> Result<(bool, Measurement, Measurement)> {
    let mut chunks = [None, None];
    let mut offsets = [0, 0];
    let mut ended = [false, false];
    let mut metrics = [Measurement::default(), Measurement::default()];
    let mut hashes = [crc32fast::Hasher::new(), crc32fast::Hasher::new()];
    let mut matched = true;
    loop {
        for side in 0..2 {
            if chunks[side].is_none() && !ended[side] {
                progress()?;
                chunks[side] = if side == 0 { source()? } else { target()? };
                match &chunks[side] {
                    Some(chunk) => {
                        let chunk = bytes(chunk);
                        if chunk.is_empty() {
                            return Err(refusal("comparison returned an empty COPY buffer"));
                        }
                        metrics[side].bytes = checked_add(metrics[side].bytes, chunk.len() as u64)?;
                        metrics[side].rows = checked_add(
                            metrics[side].rows,
                            chunk.iter().filter(|byte| **byte == b'\n').count() as u64,
                        )?;
                        hashes[side].update(chunk);
                    }
                    None => ended[side] = true,
                }
            }
        }
        match (&chunks[0], &chunks[1]) {
            (Some(left), Some(right)) => {
                let left = &bytes(left)[offsets[0]..];
                let right = &bytes(right)[offsets[1]..];
                let count = left.len().min(right.len());
                matched &= left[..count] == right[..count];
                let lengths = [left.len(), right.len()];
                for side in 0..2 {
                    if count == lengths[side] {
                        chunks[side] = None;
                        offsets[side] = 0;
                    } else {
                        offsets[side] += count;
                    }
                }
            }
            (None, None) => break,
            _ => {
                matched = false;
                chunks = [None, None];
                offsets = [0, 0];
            }
        }
    }
    let [source_hash, target_hash] = hashes;
    metrics[0].checksum = source_hash.finalize();
    metrics[1].checksum = target_hash.finalize();
    let [source, target] = metrics;
    Ok((matched && source.rows == target.rows, source, target))
}

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

#[cfg(test)]
mod tests;
