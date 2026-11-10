use crate::apply::checkpoint::{TargetAuthority, read_endpos_floor};
use crate::contracts::{
    EndposSource, Error, ErrorKind, IoControl, Result, SentinelField, StateAction, StreamCommand,
    StreamOptions,
};
use crate::pg::PgConnection;
use crate::protocol::{self, StreamReportCommand};
use crate::state::ControlHandle;

pub(crate) fn run(options: StreamOptions, io: &IoControl) -> Result<String> {
    if options.state.action != StateAction::Resume {
        return Err(refusal("stream commands require existing native state"));
    }
    if options.command == StreamCommand::Cleanup {
        return crate::cleanup::run(&options, io);
    }
    let control = ControlHandle::inspect(&options.state.path, options.operator.as_ref(), io)?;
    let request = match options.command {
        StreamCommand::Get(_) => control.read(io)?,
        StreamCommand::SetEndpos(source) => {
            let end = match source {
                EndposSource::Explicit(end) => end,
                EndposSource::Current => current_endpos(&options, &control, io)?,
            };
            let mut target = target(&options, io)?;
            let floor = read_endpos_floor(&mut target, &control, io)?;
            Some(control.set(end, floor, io)?)
        }
        StreamCommand::Cleanup => unreachable!(),
    };
    if !options.json
        && matches!(
            options.command,
            StreamCommand::Get(SentinelField::Endpos) | StreamCommand::SetEndpos(_)
        )
    {
        let request = request
            .as_ref()
            .ok_or_else(|| refusal("stream endpos has not been requested"))?;
        return Ok(format!("{}\n", protocol::lsn(request.end())));
    }
    let mut authority = TargetAuthority::try_acquire_follow(target(&options, io)?, &control, io)?;
    let snapshot = authority.observe_follow(io)?;
    let completed = if snapshot.follow_completed() {
        let request = request
            .as_ref()
            .ok_or_else(|| refusal("completed follow has no local endpos"))?;
        let sealed = control.seal(request, io)?;
        Some(authority.observe_completed(&sealed, io)?)
    } else {
        None
    };
    authority.close(io)?;
    if !options.json && options.command == StreamCommand::Get(SentinelField::ReplayLsn) {
        return Ok(format!(
            "{}\n",
            protocol::lsn(snapshot.durable_replay_lsn())
        ));
    }
    let inspection = crate::state::inspect_clone(&options.state, options.operator.as_ref(), io)?;
    let settings = inspection
        .effective_follow_settings()
        .ok_or_else(|| refusal("stream report has no stored follow settings"))?;
    protocol::replication_report(
        if matches!(options.command, StreamCommand::SetEndpos(_)) {
            StreamReportCommand::SetEndpos
        } else {
            StreamReportCommand::Get
        },
        options.operator.as_ref(),
        request.as_ref(),
        &snapshot,
        completed.as_ref(),
        settings,
    )
}

fn target(options: &StreamOptions, io: &IoControl) -> Result<PgConnection> {
    let conninfo = options.target.as_ref().ok_or_else(|| {
        Error::new(
            ErrorKind::BadArgs,
            "--target is required for measured stream state",
        )
    })?;
    PgConnection::connect(conninfo, io)
}

fn current_endpos(
    options: &StreamOptions,
    control: &ControlHandle,
    io: &IoControl,
) -> Result<crate::identity::SourceEndLsn> {
    let conninfo = options
        .source
        .as_ref()
        .ok_or_else(|| Error::new(ErrorKind::BadArgs, "--current requires --source"))?;
    let mut source = PgConnection::connect(conninfo, io)?;
    if crate::catalog::read_clone_source(&mut source, io)? != control.plan().source {
        return Err(refusal(
            "stream source identity or timeline differs from native binding",
        ));
    }
    let result = source.query(
        "SELECT pg_catalog.pg_current_wal_insert_lsn()::pg_catalog.text",
        io,
    )?;
    if result.rows() != 1 || result.columns()? != 1 {
        return Err(refusal(
            "stream source returned an invalid endpos measurement",
        ));
    }
    result
        .cell(0, 0)?
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(crate::config::parse_endpos)
        .ok_or_else(|| refusal("stream source returned an invalid endpos measurement"))
}

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}
