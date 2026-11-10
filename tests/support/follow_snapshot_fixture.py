"""Require exact current pgoutput lifecycle cases and independently bound fault receipts."""

import json
from pathlib import Path
import re

FAULT_CASES = ("create_reply_lost", "create_tail_lost", "expired_export_refused",
               "imported_identity_mismatch", "source_finish_unknown")
TAIL_PHASES = ("tail_lost", "ready_transaction", "ready_failed")
EXPIRY_STATES = {160015: "22023", 180006: "42704"}
PHASE_FIELDS = {
    "owner", "profile", "case", "phase", "status", "server_version_num", "native_path", "stream",
    "publication", "slot", "source_slot_phase", "slot_nonce", "publication_before_slot",
    "intent_before_create", "sql_pid", "exporter_pid", "helper_pid", "armed", "fault_hits",
    "client_failed", "fault_before_deadline", "failure_before_deadline", "deadline_seconds",
    "sql_closed", "exporter_closed", "helper_reaped", "helper_returncode", "retry_refused",
    "create_dispatches", "details",
}
DETAIL_FIELDS = {
    "import_seen", "imported", "identity_before", "identity_after", "row_seen", "row_delivered",
    "completion_seen", "command_tag", "ready_before", "ready_after", "client_closed_after_ready",
    "exporter_expired", "sqlstate",
}
NATIVE_FIELDS = {
    "magic", "format_version", "run", "canonical_path", "directory_device", "directory_inode",
    "copy_complete", "target_provenance", "source_slot", "follow_state", "clone_state", "operator",
}


def validate_identity_modes(plan):
    assert isinstance(plan, dict) and isinstance(plan.get("tables"), list), "missing native table identity plan"
    fields = {"source_oid", "name", "writable_columns", "all_columns", "identity_modes", "partition_identity_modes"}
    for table in plan["tables"]:
        assert isinstance(table, dict) and set(table) == fields, "native table identity fields differ"
        columns = table["all_columns"]
        assert isinstance(columns, list) and all(isinstance(name, str) for name in columns), "invalid native identity columns"
        for key in ("identity_modes", "partition_identity_modes"):
            modes = table[key]
            if key == "partition_identity_modes" and modes is None:
                continue
            assert isinstance(modes, list) and len(modes) == len(columns), "native identity mode width differs"
            assert all(type(mode) is int and mode in (0, 97, 100) for mode in modes), "invalid native identity mode"


def unique_fields(fields):
    result = dict(fields)
    assert len(result) == len(fields), "duplicate follow snapshot receipt field"
    return result


def read_json(path):
    path = Path(path)
    assert not path.is_symlink() and path.is_file(), "missing or replaced follow snapshot evidence"
    assert 0 < path.stat().st_size <= 1024 * 1024, "follow snapshot evidence exceeds its bound"
    return json.loads(path.read_bytes(), object_pairs_hook=unique_fields)


def follow_snapshot_cases():
    values = read_json(Path(__file__).with_name("follow_snapshot_cases.json"))
    assert isinstance(values, list) and len(values) == len(set(values)) == 9, "wrong follow snapshot case inventory"
    assert all(isinstance(value, str) and re.fullmatch(r"snapshot::follow_fault_tests::live_follow_[a-z_]+", value)
               for value in values), "invalid follow snapshot test name"
    return values


def follow_snapshot_application_name(context):
    token = context["token"]
    assert isinstance(token, str) and re.fullmatch(r"[0-9a-f]{32}", token), "invalid owned snapshot token"
    return "rpgmigrate_follow_snapshot_" + token[:12]


def positive(value, maximum=2**64 - 1):
    return type(value) is int and 0 < value <= maximum


def identifier(value):
    return isinstance(value, list) and len(value) == 16 and all(type(byte) is int and 0 <= byte <= 255 for byte in value) and any(value)


def lsn(value):
    assert isinstance(value, str) and re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", value), "invalid slot consistent point"
    high, low = (int(part, 16) for part in value.split("/"))
    assert high <= 0xffffffff and low <= 0xffffffff, "slot consistent point overflows uint64"
    return (high << 32) | low


def version(value, context):
    assert type(value) is int and value in EXPIRY_STATES, "unqualified follow snapshot server version"
    assert value == context["server_version_num"], "follow snapshot receipt server differs from its owned cluster"


def validate_native(value, context, case):
    root = Path(context["root"]).resolve(strict=True)
    assert (root / ".e1-owner").read_text() == context["token"], "foreign follow snapshot root"
    path = Path(value["native_path"])
    assert path.is_absolute() and str(path.resolve(strict=True)) == str(path), "noncanonical native evidence path"
    assert path.name == "manifest.json" and path.parent.parent == root, "native evidence is outside the owned fixture"
    native = read_json(path)
    assert isinstance(native, dict) and set(native) == NATIVE_FIELDS, "native follow snapshot fields differ"
    assert native["magic"] == "RPGMIGRATE" and type(native["format_version"]) is int and native["format_version"] in (8, 9), "wrong native follow snapshot format"
    assert native["canonical_path"] == str(path.parent), "native follow snapshot path binding differs"
    assert native["copy_complete"] is None and native["target_provenance"] == [] and native["clone_state"] is None and native["operator"] is None, "direct source lifecycle evidence fabricated target or completion state"
    assert positive(native["directory_device"]) and positive(native["directory_inode"]), "native directory identity is missing"
    stream = value["stream"]
    assert isinstance(stream, dict) and set(stream) == {"run", "source", "generation"}, "invalid follow stream fields"
    assert identifier(stream["run"]) and native["run"] == stream["run"] and type(stream["generation"]) is int and stream["generation"] == 1, "foreign follow stream"
    source = stream["source"]
    assert isinstance(source, dict) and set(source) == {"system_identifier", "database_oid"}, "invalid source identity fields"
    assert positive(source["system_identifier"]) and positive(source["database_oid"], 0xffffffff), "invalid source identity"
    follow = native["follow_state"]
    assert isinstance(follow, dict) and set(follow) == {"origin_name", "publication", "binding", "restart", "preliminary", "cleanup"}, "follow source state fields differ"
    assert follow["cleanup"] is None and follow["origin_name"] is None and follow["restart"] is None, "unexpected source origin or restart authority"
    publication = follow["publication"]
    assert isinstance(publication, dict) and set(publication) == {"phase", "intent", "receipt"} and publication["phase"] == "confirmed", "publication creation is not positively confirmed"
    receipt = value["publication"]
    assert isinstance(receipt, dict) and set(receipt) == {"name", "oid", "owner_oid"}, "invalid publication receipt fields"
    assert isinstance(receipt["name"], str) and re.fullmatch(r"[a-z_][a-z_0-9]{0,62}", receipt["name"]), "invalid fixture publication name"
    assert positive(receipt["oid"], 0xffffffff) and positive(receipt["owner_oid"], 0xffffffff), "invalid publication object identity"
    assert publication["receipt"] == receipt, "publication object identity differs from native ownership"
    intent = publication["intent"]
    assert isinstance(intent, dict) and set(intent) == {"run", "name", "nonce"}, "invalid publication intent fields"
    assert intent["run"] == stream["run"] and intent["name"] == receipt["name"] and identifier(intent["nonce"]), "foreign publication intent"
    create_loss = case in ("create_reply_lost", "create_tail_lost")
    slot_state = native["source_slot"]
    expected_phase = "provisioning" if create_loss else "confirmed"
    assert isinstance(slot_state, dict) and set(slot_state) == {"phase", "record"}, "invalid native source slot fields"
    assert value["source_slot_phase"] == slot_state["phase"] == expected_phase, "wrong source slot creation phase"
    slot_record = slot_state["record"]
    if create_loss:
        slot_intent = slot_record
    else:
        assert isinstance(slot_record, dict) and set(slot_record) == {"intent", "consistent_point", "snapshot_token"}, "confirmed source slot fields differ"
        assert isinstance(slot_record["snapshot_token"], str) and 0 < len(slot_record["snapshot_token"]) <= 4096, "confirmed source snapshot token is missing"
        slot_intent = slot_record["intent"]
    assert isinstance(slot_intent, dict) and set(slot_intent) == {"binding", "nonce"}, "invalid source slot intent fields"
    assert identifier(value["slot_nonce"]) and slot_intent["nonce"] == value["slot_nonce"], "foreign source slot nonce"
    binding = slot_intent["binding"]
    assert isinstance(binding, dict) and set(binding) == {"stream", "slot_name", "plugin", "catalog_epoch"}, "invalid source slot binding fields"
    slot = value["slot"]
    assert isinstance(slot, dict) and set(slot) == {"name", "plugin", "database", "database_oid", "temporary", "consistent_point"}, "invalid observed source slot fields"
    assert isinstance(slot["name"], str) and re.fullmatch(r"[a-z_][a-z_0-9]{0,62}", slot["name"]), "invalid fixture slot name"
    assert slot["database"] == path.parent.name and positive(slot["database_oid"], 0xffffffff) and slot["database_oid"] == source["database_oid"], "foreign source slot database"
    assert slot["temporary"] is False and slot["plugin"] == binding["plugin"] == "pgoutput", "wrong source slot plugin or persistence"
    assert binding["stream"] == stream and binding["slot_name"] == slot["name"] and positive(binding["catalog_epoch"]), "foreign source slot binding"
    point = lsn(slot["consistent_point"])
    assert point > 0, "source slot consistent point is absent"
    if not create_loss:
        assert type(slot_record["consistent_point"]) is int and slot_record["consistent_point"] == point, "source slot consistent point differs from native receipt"
    plan = follow["preliminary"]
    validate_identity_modes(plan)
    assert isinstance(plan, dict) and type(plan.get("generation")) is int and plan["generation"] == binding["catalog_epoch"], "source preliminary plan generation differs"
    assert plan["source"]["identity"] == source and plan["source"]["major"] == value["server_version_num"] // 10000, "source preliminary plan identity differs"
    assert follow["binding"] is None, "direct source lifecycle fixture contains a later coordinator binding"


def validate_phase(value, context, case, phase):
    assert isinstance(value, dict) and set(value) == PHASE_FIELDS, "follow snapshot phase fields differ"
    assert value["owner"] == context["token"] and value["profile"] == "follow_snapshot" and value["case"] == case and value["phase"] == phase, "foreign follow snapshot fault receipt"
    assert value["status"] == "observed", "pending follow snapshot fault receipt"
    version(value["server_version_num"], context)
    assert type(value["fault_hits"]) is int and value["fault_hits"] == 1 and type(value["create_dispatches"]) is int and value["create_dispatches"] == 1, "missing or repeated intended CREATE/fault"
    for key in ("publication_before_slot", "intent_before_create", "armed", "client_failed", "fault_before_deadline", "failure_before_deadline", "sql_closed", "exporter_closed", "helper_reaped"):
        assert value[key] is True, "missing ordered creation, closure, deadline, or helper evidence"
    assert type(value["helper_returncode"]) is int and value["helper_returncode"] == 0, "follow snapshot helper failed"
    assert type(value["deadline_seconds"]) is int and value["deadline_seconds"] == 20, "wrong follow snapshot fault deadline"
    for key in ("sql_pid", "exporter_pid", "helper_pid"):
        assert positive(value[key], 0x7fffffff), "missing actual follow snapshot process identity"
    assert value["sql_pid"] != value["exporter_pid"], "source SQL and exporter sessions are not distinct"
    create_loss = case in ("create_reply_lost", "create_tail_lost")
    assert value["retry_refused"] is create_loss, "CREATE retry refusal is missing or fabricated"
    validate_native(value, context, case)
    details = value["details"]
    assert isinstance(details, dict) and set(details) == DETAIL_FIELDS, "follow snapshot fault details differ"
    for key in ("import_seen", "imported", "row_seen", "row_delivered", "completion_seen", "client_closed_after_ready", "exporter_expired"):
        assert type(details[key]) is bool, "nonboolean follow snapshot event observation"
    assert details["import_seen"] is (not create_loss), "fault did not reach its intended import boundary"
    assert details["imported"] is (case in ("imported_identity_mismatch", "source_finish_unknown")), "wrong positive import observation"
    if create_loss:
        assert details["row_seen"] is True and details["completion_seen"] is True and details["command_tag"] == "CREATE_REPLICATION_SLOT", "actual source slot CREATE was not observed"
        assert details["row_delivered"] is (case == "create_tail_lost") and details["ready_before"] == "I", "wrong source CREATE row/tail delivery"
        changed = {"ready_transaction": "T", "ready_failed": "E"}.get(phase)
        assert details["ready_after"] == changed and details["client_closed_after_ready"] is (changed is not None), "edited CREATE readiness did not force client closure"
    else:
        assert details["ready_before"] is None and details["ready_after"] is None and details["client_closed_after_ready"] is False, "unrelated CREATE tail evidence"
        assert details["row_seen"] is False and details["row_delivered"] is False and details["completion_seen"] is False, "unrelated CREATE-only row evidence"
        assert details["command_tag"] == ("COMMIT" if case == "source_finish_unknown" else None), "wrong imported source completion fault"
    assert details["exporter_expired"] is (case == "expired_export_refused"), "wrong owned exporter expiry observation"
    assert details["sqlstate"] == (EXPIRY_STATES[value["server_version_num"]] if case == "expired_export_refused" else None), "wrong actual source expiry SQLSTATE"
    if case == "imported_identity_mismatch":
        assert details["identity_before"] == value["sql_pid"] and positive(details["identity_after"], 0x7fffffff) and details["identity_after"] != details["identity_before"], "intended post-import SQL PID mutation is missing"
    else:
        assert details["identity_before"] is None and details["identity_after"] is None, "unexpected imported identity mutation"


def validate_fault(value, context, case):
    assert case in FAULT_CASES, "unknown follow snapshot fault case"
    if case != "create_tail_lost":
        validate_phase(value, context, case, case)
        return
    assert isinstance(value, dict) and set(value) == {"owner", "profile", "case", "status", "server_version_num", "fault_hits", "phases"}, "follow snapshot tail aggregate fields differ"
    assert value["owner"] == context["token"] and value["profile"] == "follow_snapshot" and value["case"] == case and value["status"] == "observed", "foreign or pending follow snapshot tail aggregate"
    version(value["server_version_num"], context)
    assert type(value["fault_hits"]) is int and value["fault_hits"] == 3, "missing or repeated CREATE tail fault"
    phases = value["phases"]
    assert isinstance(phases, dict) and set(phases) == set(TAIL_PHASES), "missing or extra source CREATE tail phase"
    for phase in TAIL_PHASES:
        validate_phase(phases[phase], context, case, phase)
    assert len({phase["slot"]["name"] for phase in phases.values()}) == 3 and len({tuple(phase["slot_nonce"]) for phase in phases.values()}) == 3, "CREATE tail phases reused source resource authority"


def validate_follow_snapshot_faults(context):
    root = Path(context["root"])
    expected = {f"follow-snapshot-fault-{case}.json" for case in FAULT_CASES}
    assert {path.name for path in root.glob("follow-snapshot-fault-*.json")} == expected, "missing or extra follow snapshot fault receipts"
    for case in FAULT_CASES:
        validate_fault(read_json(root / f"follow-snapshot-fault-{case}.json"), context, case)
