"""Negative controls for the cutover component qualification inventory."""

from contextlib import nullcontext
import json
from pathlib import Path
import runpy
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import authority_fixture as authority
import authority_fault

from db_harness import module_cases

ROOT = Path(__file__).resolve().parents[2]
RECEIVE_ADDITIONS = tuple("cdc::receive::tests::" + name for name in (
    "drain_request_retries_and_coalesces_without_advancing_feedback",
    "idle_drain_request_wakes_before_the_status_timer_without_advancing_feedback",
    "completed_stop_requires_checked_terminal_results",
    "completed_stop_during_partial_later_input_checks_end_without_decoding_the_tail",
    "completed_stop_drains_pending_status_before_ending_and_keeps_reply_intent",
    "completed_stop_while_fifo_full_preserves_order_and_unapplied_feedback_floor",
    "completed_stop_waiting_for_terminals_preserves_deadline_and_cancellation",
))
RECEIVE_BASELINE = (
    "cdc::receive::tests::live_configuration_and_logical_query_preserve_export_lifetime",
    "pg::follow_receive_tests::live_pipeline_commands_counts_dispatched_commands",
    "cdc::receive::tests::live_startup_refuses_slot_advanced_beyond_recovery_before_readiness",
    "cdc::receive::tests::full_fifo_status_survives_walsender_timeout",
)

AUTHORITY_BASELINE = (
    "apply::checkpoint::live_tests::follow_streamed_group_is_atomic_and_checkpointed_once",
    "apply::checkpoint::live_tests::follow_cas_failure_rolls_back_dml_and_feedback",
    "apply::checkpoint::live_tests::follow_certification_and_recovery_refuse_a_source_gap",
    "apply::checkpoint::live_tests::follow_cancelled_sync_wait_never_proves_group_or_certification",
    "apply::checkpoint::live_tests::follow_restart_resets_the_complete_workset_and_allows_safe_preworker_retry",
    "apply::checkpoint::live_tests::follow_restart_refuses_foreign_replacement_and_fk_without_mutating_rows",
    "apply::checkpoint::live_tests::follow_identity_rewrite_commits_data_and_checkpoint",
    "apply::checkpoint::live_tests::follow_identity_rewrite_constraint_failure_rolls_back",
    "apply::checkpoint::live_tests::follow_identity_rewrite_missing_row_and_wrong_tag_roll_back",
    "apply::checkpoint::live_tests::follow_identity_rewrite_cas_failure_rolls_back",
    "apply::checkpoint::live_tests::follow_identity_rewrite_transaction_escape_rolls_back",
)
AUTHORITY_ADDITIONS = tuple("apply::checkpoint::live_tests::" + name for name in (
    "follow_endpos_floor_is_unfenced_and_certification_independent",
    "follow_command_observation_is_existing_only_and_same_value",
    "follow_drain_completion_syncs_final_sequences_and_is_immutable",
    "follow_drain_recovery_reproves_original_witness",
    "follow_observation_unknown_commit_needs_new_barrier",
    "follow_cancelled_sync_wait_never_proves_drain_completion_or_observation",
))

SOURCE_BASELINE = (
    "snapshot::follow_tests::live_follow_slot_snapshot_preserves_initial_boundary_and_exporter_lifetime",
    "snapshot::follow_tests::live_follow_generated_columns_refuse_before_resource_creation",
    "snapshot::follow_tests::live_follow_publication_membership_and_type_map_are_exact",
    "snapshot::follow_tests::live_follow_colliding_names_and_wrong_resource_oids_are_refused",
    "snapshot::follow_tests::live_follow_relation_uses_target_type_oids_and_exact_owned_identity",
    "snapshot::follow_tests::live_follow_executed_expressions_refuse_before_resource_creation",
    "snapshot::follow_tests::live_follow_positive_publication_retry_requires_the_persisted_source_plan",
)
SOURCE_ADDITIONS = tuple("catalog::follow::tests::" + name for name in (
    "live_cleanup_owned_advanced_slot_and_publication",
    "live_cleanup_publication_identity_options_membership_refuse",
    "live_cleanup_active_or_foreign_slot_survives",
    "live_cleanup_recorded_removal_reappearance_survives",
    "live_cleanup_source_binding_mismatch_preserves_resources",
))


class FollowM4HarnessTests(unittest.TestCase):
    def test_authority_source_is_separate_owned_database_with_admin_connection(self):
        context = {"token": "owned"}
        cluster = SimpleNamespace(context=context, manifest={})
        unsafe = SimpleNamespace(context={"token": "unsafe"}, token="unsafe")
        commands = []
        with patch.object(authority, "assert_identity"), \
                patch.object(authority, "psql", side_effect=lambda ctx, db, sql: commands.append((ctx, db, sql))), \
                patch.object(authority, "conninfo", side_effect=lambda ctx, db, role: ctx["token"] + ":" + db + ":" + role), \
                patch.object(authority, "owned_standby", return_value=nullcontext({"token": "standby"})), \
                patch.object(authority, "OwnedCluster", return_value=nullcontext(unsafe)):
            with authority.authority_fixture(cluster) as environment:
                self.assertEqual(environment["RPGMIGRATE_TEST_AUTHORITY_SOURCE"], "owned:e3_authority_source:e1_admin")
                self.assertNotEqual(environment["RPGMIGRATE_TEST_AUTHORITY_SOURCE"], environment["RPGMIGRATE_TEST_AUTHORITY_ADMIN"])
        self.assertIn((context, "postgres", "CREATE DATABASE e3_authority_source OWNER e1_target TEMPLATE template0 ENCODING 'UTF8';"), commands)
        self.assertIn((context, "postgres", "COMMENT ON DATABASE e3_authority_source IS 'owned';"), commands)

    def test_authority_fault_inventories_preserve_legacy_and_require_each_cutover_stage(self):
        self.assertEqual(authority_fault.FAULT_CASES, ("lost_commit_reply", "provisioning_recovery"))
        self.assertEqual(authority_fault.FOLLOW_FAULT_CASES, ("follow_drain", "follow_completion", "follow_observation"))
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned"}
            for case in authority_fault.FOLLOW_FAULT_CASES:
                value = {"status": "observed", "owner": "owned", "case": case, "fault_hits": 1,
                         "commit_tag": "COMMIT", "backend_pid": 42, "held_fence_observed": True,
                         "released_fence_observed": True, "backend_connection_closed": True, "holding_backend": False}
                (Path(directory) / f"authority-fault-{case}.json").write_text(json.dumps(value))
            authority.validate_authority_faults(context, follow=True)
            for case in authority_fault.FOLLOW_FAULT_CASES:
                path = Path(directory) / f"authority-fault-{case}.json"
                original = path.read_text()
                path.unlink()
                with self.subTest(case=case), self.assertRaises(AssertionError):
                    authority.validate_authority_faults(context, follow=True)
                path.write_text(original)
            with self.assertRaises(AssertionError):
                authority.validate_authority_faults(context)

    def test_authority_profile_requires_real_fault_receipts_after_exact_success_summary(self):
        validate = runpy.run_path(str(ROOT / "tools/test-db.py"))["validate_module_receipt"]
        cases = module_cases({}, "FollowAuthority")
        output = "".join(f"test {name} ... ok\n" for name in cases)
        output += f"test result: ok. {len(cases)} passed; 0 failed; 0 ignored; 0 measured; 600 filtered out; finished in 0.01s\n"
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned"}
            with self.assertRaises(AssertionError):
                validate("FollowAuthority", cases, output, context)
            for case in authority_fault.FOLLOW_FAULT_CASES:
                value = {"status": "observed", "owner": "owned", "case": case, "fault_hits": 1,
                         "commit_tag": "COMMIT", "backend_pid": 42, "held_fence_observed": True,
                         "released_fence_observed": True, "backend_connection_closed": True, "holding_backend": False}
                (Path(directory) / f"authority-fault-{case}.json").write_text(json.dumps(value))
            validate("FollowAuthority", cases, output, context)
            for case in cases:
                with self.subTest(case=case), self.assertRaises(AssertionError):
                    validate("FollowAuthority", cases, output.replace(f"test {case} ... ok\n", ""), context)

    def test_authority_inventory_preserves_eleven_and_requires_six_cutover_cases(self):
        self.assertEqual(module_cases({}, "FollowAuthority"), [*AUTHORITY_BASELINE, *AUTHORITY_ADDITIONS])

    def test_source_inventory_preserves_seven_and_requires_five_cleanup_cases(self):
        self.assertEqual(module_cases({}, "FollowSource"), [*SOURCE_BASELINE, *SOURCE_ADDITIONS])

    def test_receive_inventory_preserves_four_cases_and_requires_seven_cutover_cases(self):
        self.assertEqual(module_cases({}, "FollowReceive"), [*RECEIVE_BASELINE, *RECEIVE_ADDITIONS])

    def test_receive_receipt_rejects_each_missing_duplicate_ignored_or_failed_case(self):
        validate = runpy.run_path(str(ROOT / "tools/test-db.py"))["validate_module_receipt"]
        cases = [*RECEIVE_BASELINE, *RECEIVE_ADDITIONS]
        records = "".join(f"test {name} ... ok\n" for name in cases)
        summary = f"test result: ok. {len(cases)} passed; 0 failed; 0 ignored; 0 measured; 600 filtered out; finished in 0.01s\n"
        validate("FollowReceive", cases, records + summary, {})
        for case in cases:
            record = f"test {case} ... ok\n"
            for bad in ((records + summary).replace(record, ""),
                        records + record + summary,
                        (records + summary).replace(record, record.replace("ok", "ignored")),
                        (records + summary).replace(record, record.replace("ok", "FAILED"))):
                with self.subTest(case=case), self.assertRaises(AssertionError):
                    validate("FollowReceive", cases, bad, {})


if __name__ == "__main__":
    unittest.main()
