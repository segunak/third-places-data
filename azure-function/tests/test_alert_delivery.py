"""Alert delivery regression tests. SMTP and GitHub are always mocked."""

import base64
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import smtplib
import sys
from typing import Callable
from unittest import mock
from urllib.error import HTTPError

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts import alert_delivery as alerts


class FakeMemory:
    def __init__(self, state=None):
        self.state = copy.deepcopy(state if state is not None else {
            "items": [],
            "_delivery": {"recoveries": {alerts.RECOVERY_ID: {"sent_at": "earlier"}}},
            "unrelated": {"keep": True},
        })
        self.writes = []

    def read(self):
        return copy.deepcopy(self.state), "sha"

    def update(self, transition):
        self.state = transition(copy.deepcopy(self.state))
        self.writes.append(copy.deepcopy(self.state))
        return copy.deepcopy(self.state)


@pytest.fixture
def manifest():
    return alerts.load(alerts.RECOVERY)


@pytest.fixture
def candidate():
    return {
        "title_hash": "abc123",
        "title": "Neighborhood cafe opens in Charlotte",
        "source": "Local News",
        "url": "https://local.example.net/new-cafe",
        "description": "A Charlotte cafe and community gathering space opens soon.",
        "location_evidence": ["Charlotte"],
        "source_kind": "feed",
    }


def selection_for(candidate, kind="news"):
    return [{"id": alerts.identity(candidate, kind), "category": "openings-cafe",
             "evidence": candidate["title"]}]


def request_for(selection):
    request = {"type": "send_email_report",
               "selection": base64.b64encode(json.dumps(selection, ensure_ascii=False).encode()).decode()}
    return {"items": [request], "errors": []}, json.dumps(request) + "\n"


def snapshot_for(candidate, mode="real"):
    return {"mode": mode, "candidates": [candidate], "replay": None}


def run_delivery(memory, candidate, manifest, sender=None, mode="real", **kwargs):
    output, raw = request_for(selection_for(candidate))
    return alerts.deliver(memory, output, raw, snapshot_for(candidate, mode),
                          "news", mode, manifest, sender or mock.Mock(), "run-1", **kwargs)


class TestOutputValidation:
    def test_encoded_source_mentions_survive_string_input_sanitization(self, manifest):
        selected = [{"id": item["title_hash"], "category": item["relevance_category"],
                     "evidence": item["title"]} for item in manifest["items"]]
        output, raw = request_for(selected)
        assert "@The" not in raw  # gh-aw's mention sanitizer cannot alter the transport.
        assert alerts.validate_queue(output, raw) == selected
        assert "@The Milestone" in selected[-1]["evidence"]

    @pytest.mark.parametrize("bad_selection", ["not base64", "W10=", "eyJub3QiOiJsaXN0In0="])
    def test_invalid_or_empty_encoded_selection_fails(self, bad_selection):
        item = {"type": "send_email_report", "selection": bad_selection}
        with pytest.raises(ValueError):
            alerts.validate_queue({"items": [item]}, json.dumps(item))

    @pytest.mark.parametrize("errors", [["Line 2: maximum allowed 1"], {"bad": "request"}])
    def test_ingestion_errors_reject_even_one_retained_email(self, candidate, errors):
        output, raw = request_for(selection_for(candidate))
        output["errors"] = errors
        with pytest.raises(ValueError, match="ingestion errors"):
            alerts.validate_queue(output, raw)

    def test_duplicate_raw_requests_rejected_after_ingestion_truncation(self, candidate):
        output, raw = request_for(selection_for(candidate))
        with pytest.raises(ValueError, match="Exactly one"):
            alerts.validate_queue(output, raw + raw)

    def test_duplicate_retained_requests_rejected(self, candidate):
        output, raw = request_for(selection_for(candidate))
        output["items"] *= 2
        with pytest.raises(ValueError, match="Exactly one"):
            alerts.validate_queue(output, raw)

    @pytest.mark.parametrize("raw", ["", "not-json", "{}\n", '{"type":"noop"}\n'])
    def test_missing_malformed_or_inconsistent_raw_rejected(self, candidate, raw):
        output, _ = request_for(selection_for(candidate))
        with pytest.raises((ValueError, KeyError)):
            alerts.validate_queue(output, raw)

    def test_legacy_placeholder_syntax_probe_cannot_send(self, candidate, manifest):
        memory, sender = FakeMemory(), mock.Mock()
        item = {"type": "send_email_report", "subject": "Test Subject", "text_body": "Test body"}
        with pytest.raises(ValueError, match="Agent-authored"):
            alerts.deliver(memory, {"items": [item]}, json.dumps(item),
                           snapshot_for(candidate), "news", "real", manifest, sender, "run")
        sender.assert_not_called()
        assert not memory.writes

    @pytest.mark.parametrize("field,value", [
        ("id", "invented"), ("category", "Test Subject"), ("evidence", "Test body"),
        ("evidence", "A completely fabricated venue quote"),
    ])
    def test_semantic_validation_not_just_lengths(self, candidate, field, value):
        selected = selection_for(candidate)
        selected[0][field] = value
        with pytest.raises(ValueError):
            alerts.validate_selection(selected, snapshot_for(candidate), "news", "real")

    def test_duplicate_selected_ids_rejected(self, candidate):
        with pytest.raises(ValueError, match="Duplicate"):
            alerts.validate_selection(selection_for(candidate) * 2, snapshot_for(candidate), "news", "real")

    def test_location_evidence_required(self, candidate):
        candidate["location_evidence"] = []
        with pytest.raises(ValueError, match="location"):
            alerts.validate_selection(selection_for(candidate), snapshot_for(candidate), "news", "real")

    @pytest.mark.parametrize("field,value", [
        ("source_kind", "test"), ("url", "https://example.com/a"),
        ("url", "javascript:alert(1)"), ("title", "Test: Charlotte cafe"),
        ("title", "Placeholder content with enough words to pass length"),
    ])
    def test_synthetic_or_invalid_candidate_rejected_in_real(self, candidate, field, value):
        candidate[field] = value
        with pytest.raises(ValueError):
            alerts.validate_selection(selection_for(candidate), snapshot_for(candidate), "news", "real")

    @pytest.mark.parametrize("snapshot_change", [{"error": "collector failed"}, {"mode": "test"}])
    def test_failed_collection_or_agent_claimed_mode_rejected(self, candidate, snapshot_change):
        snapshot = snapshot_for(candidate)
        snapshot.update(snapshot_change)
        with pytest.raises(ValueError, match="failed|mode"):
            alerts.validate_selection(selection_for(candidate), snapshot, "news", "real")

    def test_payload_cannot_set_mode(self, candidate):
        output, raw = request_for(selection_for(candidate))
        output["items"][0]["mode"] = "test"
        with pytest.raises(ValueError, match="modes"):
            alerts.validate_queue(output, raw)

    def test_raw_selection_must_match_retained_selection(self, candidate):
        output, raw = request_for(selection_for(candidate))
        output["items"][0]["selection"] = "[]"
        with pytest.raises(ValueError, match="disagree"):
            alerts.validate_queue(output, raw)


class TestTrustedTrigger:
    @pytest.mark.parametrize("event,inputs,expected", [
        ("schedule", {"mode": "test"}, "real"),
        ("workflow_dispatch", {"mode": "test"}, "test"),
        ("workflow_dispatch", {"mode": "real"}, "real"),
        ("workflow_dispatch", {}, "real"),
    ])
    def test_mode_only_from_supported_trigger(self, event, inputs, expected):
        assert alerts.trusted_mode(event, {"inputs": inputs}) == expected

    @pytest.mark.parametrize("event,inputs", [
        ("pull_request", {}), ("workflow_dispatch", {"mode": "probe"}),
    ])
    def test_invalid_triggers_fail(self, event, inputs):
        with pytest.raises(ValueError):
            alerts.trusted_mode(event, {"inputs": inputs})


class TestDeliveryHistory:
    def test_only_exact_sent_items_recorded_after_smtp(self, candidate, manifest):
        memory = FakeMemory()
        old = {"url": "https://local.example.net/old", "title_hash": "old", "custom": "keep"}
        memory.state["items"].append(old)
        snapshot = snapshot_for(candidate)
        snapshot["candidates"].append({**candidate, "url": "https://local.example.net/unsent", "title_hash": "unsent"})
        selected = selection_for(candidate)
        output, raw = request_for(selected)

        def send(email, delivery_id):
            assert memory.state["items"] == [old]
            assert memory.state["_delivery"]["pending"]["status"] == "sending"
            assert candidate["title"] in email["text"]
            assert candidate["url"] in email["text"]
            assert "Matched evidence:" in email["text"]
            assert "Why it matters:" in email["text"]
            assert "unsent" not in email["text"]
            assert len(delivery_id) == 64

        sender = mock.Mock(side_effect=send)
        alerts.deliver(memory, output, raw, snapshot, "news", "real", manifest, sender, "run-1")
        sender.assert_called_once()
        assert [item["title_hash"] for item in memory.state["items"]] == ["old", "abc123"]
        assert memory.state["items"][0] == old
        assert memory.state["items"][1]["last_notified_at"]
        assert memory.state["unrelated"] == {"keep": True}
        assert "pending" not in memory.state["_delivery"]
        # Rerunning the same completed report is idempotent.
        alerts.deliver(memory, output, raw, snapshot, "news", "real", manifest, sender, "run-2")
        sender.assert_called_once()

    def test_test_mode_sends_but_never_mutates_history(self, candidate, manifest):
        memory, sender = FakeMemory(), mock.Mock()
        candidate.update(title="Test: A synthetic Charlotte cafe", source_kind="test")
        before = copy.deepcopy(memory.state)
        run_delivery(memory, candidate, manifest, sender, mode="test")
        sender.assert_called_once()
        assert sender.call_args.args[0]["subject"].startswith("[TEST]")
        assert memory.state == before and not memory.writes

    def test_valid_noop_does_not_send_or_mutate(self, manifest):
        memory, sender = FakeMemory(), mock.Mock()
        item = {"type": "noop", "message": "No relevant new leads"}
        assert "noop" in alerts.deliver(
            memory, {"items": [item]}, json.dumps(item), {"mode": "real", "candidates": []},
            "news", "real", manifest, sender, "run")
        sender.assert_not_called()
        assert not memory.writes

    def test_explicit_smtp_rejection_keeps_retryable_outbox_not_history(self, candidate, manifest):
        memory = FakeMemory()
        with pytest.raises(alerts.NotAccepted):
            run_delivery(memory, candidate, manifest, mock.Mock(side_effect=alerts.NotAccepted("rejected")))
        assert memory.state["items"] == []
        assert memory.state["_delivery"]["pending"]["status"] == "prepared"
        pending = alerts.replay(memory.state, "news", manifest)
        snapshot = {"mode": "real", "candidates": pending["candidates"], "replay": pending}
        output, raw = request_for(pending["selection"])
        sender = mock.Mock()
        alerts.deliver(memory, output, raw, snapshot, "news", "real", manifest, sender, "retry")
        sender.assert_called_once()
        assert len(memory.state["items"]) == 1

    def test_ambiguous_smtp_outcome_blocks_all_automatic_retries(self, candidate, manifest):
        memory = FakeMemory()
        with pytest.raises(TimeoutError):
            run_delivery(memory, candidate, manifest, mock.Mock(side_effect=TimeoutError("DATA ack lost")))
        assert memory.state["items"] == []
        assert memory.state["_delivery"]["pending"]["status"] == "sending"
        sender = mock.Mock()
        with pytest.raises(ValueError, match="Ambiguous"):
            run_delivery(memory, candidate, manifest, sender)
        sender.assert_not_called()

    def test_receipt_write_failure_never_claims_delivery_in_history(self, candidate, manifest):
        memory = FakeMemory()
        original_update = memory.update

        def update(transition):
            if len(memory.writes) == 2:
                raise OSError("GitHub unavailable after SMTP")
            return original_update(transition)

        memory.update = update
        sender = mock.Mock()
        with pytest.raises(OSError):
            run_delivery(memory, candidate, manifest, sender)
        sender.assert_called_once()
        assert memory.state["items"] == []
        assert memory.state["_delivery"]["pending"]["status"] == "sending"

    def test_failed_stage_never_reaches_smtp(self, candidate, manifest):
        memory, sender = FakeMemory(), mock.Mock()
        memory.update = mock.Mock(side_effect=OSError("GitHub unavailable"))
        with pytest.raises(OSError):
            run_delivery(memory, candidate, manifest, sender)
        sender.assert_not_called()

    def test_concurrent_claim_cannot_send_twice(self, candidate, manifest):
        memory, sender = FakeMemory(), mock.Mock()
        update = memory.update

        def race(transition):
            if len(memory.writes) == 1:
                memory.state["_delivery"]["pending"]["status"] = "sending"
                memory.state["_delivery"]["pending"]["run_id"] = "other-run"
            return update(transition)

        memory.update = race
        with pytest.raises(ValueError, match="Concurrent"):
            run_delivery(memory, candidate, manifest, sender)
        sender.assert_not_called()
        assert memory.state["items"] == []

    def test_stale_selection_cannot_notify_previously_seen_item(self, candidate, manifest):
        memory, sender = FakeMemory(), mock.Mock()
        memory.state["items"] = [candidate]
        with pytest.raises(ValueError, match="already-notified"):
            run_delivery(memory, candidate, manifest, sender)
        sender.assert_not_called()

    def test_read_only_validation_does_not_send_or_write(self, candidate, manifest):
        memory, sender = FakeMemory(), mock.Mock()
        run_delivery(memory, candidate, manifest, sender, validate_only=True)
        sender.assert_not_called()
        assert not memory.writes

    def test_output_errors_and_duplicate_raw_never_reach_smtp(self, candidate, manifest):
        output, raw = request_for(selection_for(candidate))
        sender, memory = mock.Mock(), FakeMemory()
        for bad_output, bad_raw in [
            (dict(output, errors=["maximum allowed 1"]), raw),
            (output, raw + raw),
            ({"items": []}, ""),
        ]:
            with pytest.raises(ValueError):
                alerts.deliver(memory, bad_output, bad_raw, snapshot_for(candidate),
                               "news", "real", manifest, sender, "run")
        sender.assert_not_called()
        assert not memory.writes

    def test_reddit_uses_its_original_identity_fields(self, manifest):
        candidate = {"id": "t3_123", "text_hash": "reddit-hash",
                     "title": "A cozy Charlotte cafe for studying",
                     "permalink": "https://www.reddit.com/r/Charlotte/comments/123",
                     "text": "A cozy cafe in Charlotte", "source_query_family": "coffee-cafe"}
        memory = FakeMemory({"items": [], "other_data": ["keep"]})
        output, raw = request_for(selection_for(candidate, "reddit"))
        sender = mock.Mock()
        alerts.deliver(memory, output, raw, snapshot_for(candidate),
                       "reddit", "real", manifest, sender, "run")
        assert memory.state["items"][0]["id"] == candidate["id"]
        assert memory.state["items"][0]["text_hash"] == candidate["text_hash"]
        assert memory.state["items"][0]["source_query_family"] == "coffee-cafe"
        assert memory.state["other_data"] == ["keep"]


class TestHistoryRetention:
    @pytest.fixture
    def clock(self, monkeypatch: pytest.MonkeyPatch) -> datetime:
        reference = datetime(2026, 9, 28, tzinfo=timezone.utc)
        monkeypatch.setattr(alerts, "now", lambda: reference.isoformat())
        return reference

    @pytest.fixture
    def memory(self, clock: datetime) -> FakeMemory:
        memory = FakeMemory()
        memory.state["items"] = [
            {"title_hash": "expired", "last_notified_at": (clock - timedelta(days=91)).isoformat()},
            {"title_hash": "boundary", "last_notified_at": (clock - timedelta(days=90)).isoformat(),
             "custom": {"keep": True}},
            {"title_hash": "fallback-expired", "first_seen_at": (clock - timedelta(days=91)).isoformat()},
            {"title_hash": "recent", "first_seen_at": (clock - timedelta(days=100)).isoformat(),
             "last_notified_at": (clock - timedelta(days=1)).isoformat()},
            {"title_hash": "undated", "custom": "keep"},
            {"title_hash": "invalid-date", "last_notified_at": "unknown"},
        ]
        memory.state["_delivery"]["completed"] = {
            "expired": {"sent_at": (clock - timedelta(days=91)).isoformat(), "run_id": "old"},
            "boundary": {"sent_at": (clock - timedelta(days=90)).isoformat(), "run_id": "keep"},
        }
        return memory

    @pytest.mark.parametrize("kind", ["news", "reddit"])
    def test_success_prunes_only_expired_records(
        self, memory: FakeMemory, candidate: dict, manifest: dict, kind: str,
    ) -> None:
        before = copy.deepcopy(memory.state)
        if kind == "reddit":
            candidate = {**candidate, "id": "reddit-id", "permalink": candidate["url"]}
        output, raw = request_for(selection_for(candidate, kind))

        def send(email: dict, delivery_id: str) -> None:
            assert memory.state["items"] == before["items"]
            assert memory.state["_delivery"]["completed"] == before["_delivery"]["completed"]
            assert memory.state["_delivery"]["pending"]["status"] == "sending"

        alerts.deliver(memory, output, raw, snapshot_for(candidate), kind, "real",
                       manifest, send, "run")
        assert memory.state["items"][:-1] == [
            item for item in before["items"] if item["title_hash"] not in ("expired", "fallback-expired")
        ]
        assert "expired" not in memory.state["_delivery"]["completed"]
        assert memory.state["_delivery"]["completed"]["boundary"] == before["_delivery"]["completed"]["boundary"]
        assert memory.state["_delivery"]["recoveries"] == before["_delivery"]["recoveries"]
        assert memory.state["unrelated"] == before["unrelated"]
        assert memory.state["_delivery"]["revision"] == 1
        assert "pending" not in memory.state["_delivery"]

    def test_both_maps_are_capped_at_10000_after_success(
        self, candidate: dict, manifest: dict, clock: datetime,
    ) -> None:
        memory = FakeMemory()
        records = [{"title_hash": str(index), "last_notified_at": (
            clock - timedelta(seconds=10001 - index)).isoformat()} for index in range(10001)]
        memory.state["items"] = copy.deepcopy(records)
        memory.state["_delivery"]["completed"] = {
            item["title_hash"]: {"sent_at": item["last_notified_at"], "run_id": "old"} for item in records
        }
        run_delivery(memory, candidate, manifest)
        assert len(memory.state["items"]) == 10000
        assert len(memory.state["_delivery"]["completed"]) == 10000
        assert memory.state["items"][:-1] == records[2:]
        assert not {"0", "1"} & memory.state["_delivery"]["completed"].keys()
        assert memory.state["items"][-1]["title_hash"] == candidate["title_hash"]

    @pytest.mark.parametrize("failure", [alerts.NotAccepted("rejected"), TimeoutError("ambiguous")])
    def test_failure_never_prunes_history_receipts_or_recovery(
        self, memory: FakeMemory, candidate: dict, manifest: dict, failure: Exception,
    ) -> None:
        before = copy.deepcopy(memory.state)
        with pytest.raises(type(failure)):
            run_delivery(memory, candidate, manifest, mock.Mock(side_effect=failure))
        pending = memory.state["_delivery"].pop("pending")
        assert pending["status"] == ("prepared" if isinstance(failure, alerts.NotAccepted) else "sending")
        assert memory.state == before

    @pytest.mark.parametrize("mode", ["test", "validate", "noop"])
    def test_non_delivery_never_prunes(
        self, memory: FakeMemory, candidate: dict, manifest: dict, mode: str,
    ) -> None:
        before = copy.deepcopy(memory.state)
        if mode == "noop":
            item = {"type": "noop", "message": "No new leads"}
            alerts.deliver(memory, {"items": [item]}, json.dumps(item), snapshot_for(candidate),
                           "news", "real", manifest, mock.Mock(), "run")
        else:
            run_delivery(memory, candidate, manifest, mode="test" if mode == "test" else "real",
                         validate_only=mode == "validate")
        assert memory.state == before
        assert not memory.writes

    def test_receipt_failure_preserves_old_history_and_sending_outbox(
        self, memory: FakeMemory, candidate: dict, manifest: dict,
    ) -> None:
        before = copy.deepcopy(memory.state)
        update = memory.update

        def fail_receipt(transition: Callable[[dict], dict]) -> dict:
            if len(memory.writes) == 2:
                raise OSError("Receipt write failed")
            return update(transition)

        memory.update = fail_receipt
        with pytest.raises(OSError, match="Receipt"):
            run_delivery(memory, candidate, manifest)
        assert memory.state["_delivery"].pop("pending")["status"] == "sending"
        assert memory.state == before

    @pytest.mark.parametrize("expiry", ["age", "count"])
    def test_evicted_receipt_cannot_allow_old_snapshot_to_resend(
        self, candidate: dict, manifest: dict, clock: datetime,
        monkeypatch: pytest.MonkeyPatch, expiry: str,
    ) -> None:
        memory, sender = FakeMemory(), mock.Mock()
        run_delivery(memory, candidate, manifest, sender)
        old_id = sender.call_args.args[1]
        if expiry == "age":
            monkeypatch.setattr(alerts, "now", lambda: (clock + timedelta(days=91)).isoformat())
        else:
            monkeypatch.setattr(alerts, "HISTORY_LIMIT", 1)
        next_candidate = {**candidate, "title_hash": "next", "url": candidate["url"] + "-next"}
        output, raw = request_for(selection_for(next_candidate))
        snapshot = {**snapshot_for(next_candidate), "history_revision": 1}
        alerts.deliver(memory, output, raw, snapshot, "news", "real", manifest, sender, "next")
        assert old_id not in memory.state["_delivery"]["completed"]
        assert candidate["title_hash"] not in [item["title_hash"] for item in memory.state["items"]]
        before = copy.deepcopy(memory.state)
        with pytest.raises(ValueError, match="Stale history snapshot"):
            run_delivery(memory, candidate, manifest, sender)
        assert sender.call_count == 2
        assert memory.state == before
        # The retained exact receipt remains idempotent despite the revision change.
        alerts.deliver(memory, output, raw, snapshot, "news", "real", manifest, sender, "retry")
        assert sender.call_count == 2

    def test_stage_rechecks_revision_after_concurrent_receipt(
        self, candidate: dict, manifest: dict,
    ) -> None:
        memory, sender = FakeMemory(), mock.Mock()
        update = memory.update

        def race(transition: Callable[[dict], dict]) -> dict:
            memory.state["_delivery"]["revision"] = 1
            return update(transition)

        memory.update = race
        with pytest.raises(ValueError, match="Stale history snapshot"):
            run_delivery(memory, candidate, manifest, sender)
        sender.assert_not_called()
        assert "pending" not in memory.state["_delivery"]

    def test_prepared_outbox_does_not_expire(
        self, memory: FakeMemory, candidate: dict, manifest: dict,
        clock: datetime, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with pytest.raises(alerts.NotAccepted):
            run_delivery(memory, candidate, manifest, mock.Mock(side_effect=alerts.NotAccepted("rejected")))
        pending = copy.deepcopy(memory.state["_delivery"]["pending"])
        monkeypatch.setattr(alerts, "now", lambda: (clock + timedelta(days=365)).isoformat())
        assert alerts.replay(memory.state, "news", manifest) == pending["report"]
        snapshot = {"mode": "real", "candidates": pending["report"]["candidates"],
                    "replay": pending["report"]}
        output, raw = request_for(pending["report"]["selection"])
        sender = mock.Mock()
        alerts.deliver(memory, output, raw, snapshot, "news", "real", manifest, sender, "retry")
        sender.assert_called_once()
        assert sender.call_args.args[1] == pending["id"]
        assert memory.state["_delivery"]["recoveries"][alerts.RECOVERY_ID] == {"sent_at": "earlier"}

    def test_receipt_reapplication_preserves_concurrent_fields_and_one_revision(
        self, memory: FakeMemory, candidate: dict, manifest: dict,
    ) -> None:
        update = memory.update

        def conflict(transition: Callable[[dict], dict]) -> dict:
            if len(memory.writes) == 2:
                first_attempt = transition(copy.deepcopy(memory.state))
                memory.state["concurrent_metadata"] = {"keep": True}
                committed = update(transition)
                assert committed["_delivery"] == first_attempt["_delivery"]
                return committed
            return update(transition)

        memory.update = conflict
        sender = mock.Mock()
        run_delivery(memory, candidate, manifest, sender)
        sender.assert_called_once()
        assert memory.state["_delivery"]["revision"] == 1
        assert memory.state["concurrent_metadata"] == {"keep": True}

    def test_seal_takes_revision_from_trusted_history(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, candidate: dict, manifest: dict,
    ) -> None:
        monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
        monkeypatch.setenv("GITHUB_EVENT_PATH", str(tmp_path / "event.json"))
        monkeypatch.setattr(sys, "argv", [
            "alert_delivery.py", "seal", "--kind", "news", "--directory", str(tmp_path),
        ])
        collected = {**snapshot_for(candidate), "history_revision": 999}
        with mock.patch.object(alerts, "load", side_effect=[
            {}, manifest, collected, None, {"items": [], "_delivery": {"revision": 7}},
        ]):
            alerts.main()
        assert alerts.load(tmp_path / "candidates.json")["history_revision"] == 7


class TestIncidentRecovery:
    def incident_state(self, manifest):
        old = [{"url": f"https://local.example.net/{i}", "title_hash": f"old-{i}"} for i in range(134)]
        false = [dict(item, first_seen_at=manifest["false_notified_at"],
                      last_notified_at=manifest["false_notified_at"]) for item in manifest["items"]]
        return {"items": old + false, "other_memory": {"preserve": True}}

    def test_exact_14_replayed_then_normal_processing_unblocked(self, manifest):
        assert len(manifest["items"]) == 14
        assert len({item["title_hash"] for item in manifest["items"]}) == 14
        memory = FakeMemory(self.incident_state(manifest))
        pending = alerts.replay(memory.state, "news", manifest)
        snapshot = {"mode": "real", "candidates": pending["candidates"], "replay": pending}
        output, raw = request_for(pending["selection"])

        def send(email, _):
            assert len(memory.state["items"]) == 134
            assert "Recovery 36350803135" in email["subject"]
            assert "historical" in email["text"]
            assert all(item["url"] in email["text"] for item in manifest["items"])
            assert alerts.RECOVERY_ID not in memory.state["_delivery"].get("recoveries", {})

        sender = mock.Mock(side_effect=send)
        alerts.deliver(memory, output, raw, snapshot, "news", "real", manifest, sender, "recovery")
        assert len(memory.state["items"]) == 148
        assert memory.state["other_memory"] == {"preserve": True}
        assert all(item["last_notified_at"] != manifest["false_notified_at"]
                   for item in memory.state["items"][134:])
        assert alerts.replay(memory.state, "news", manifest) is None
        alerts.deliver(memory, output, raw, snapshot, "news", "real", manifest, sender, "retry")
        sender.assert_called_once()

    def test_failed_recovery_is_durable_and_never_falsely_notified(self, manifest):
        memory = FakeMemory(self.incident_state(manifest))
        pending = alerts.replay(memory.state, "news", manifest)
        snapshot = {"mode": "real", "candidates": pending["candidates"], "replay": pending}
        output, raw = request_for(pending["selection"])
        with pytest.raises(alerts.NotAccepted):
            alerts.deliver(memory, output, raw, snapshot, "news", "real", manifest,
                           mock.Mock(side_effect=alerts.NotAccepted("rejected")), "run")
        assert len(memory.state["items"]) == 134
        restored = alerts.replay(memory.state, "news", manifest)
        assert restored["selection"] == pending["selection"]
        assert restored["candidates"] == manifest["items"]
        assert not memory.state["_delivery"].get("recoveries")

    def test_recovery_cannot_be_omitted_or_shortened(self, candidate, manifest):
        memory, sender = FakeMemory(self.incident_state(manifest)), mock.Mock()
        with pytest.raises(ValueError, match="before normal"):
            run_delivery(memory, candidate, manifest, sender)
        pending = alerts.replay(memory.state, "news", manifest)
        with pytest.raises(ValueError, match="exactly"):
            alerts.validate_selection(pending["selection"][:-1],
                                      {"mode": "real", "candidates": pending["candidates"], "replay": pending},
                                      "news", "real")
        item = {"type": "noop", "message": "No news"}
        with pytest.raises(ValueError, match="cannot be bypassed"):
            alerts.deliver(memory, {"items": [item]}, json.dumps(item),
                           {"mode": "real", "candidates": []}, "news", "real", manifest, sender, "run")
        sender.assert_not_called()
        assert not memory.writes

    def test_prepare_is_read_only_and_does_not_trust_agent_files(self, tmp_path, manifest):
        memory = FakeMemory(self.incident_state(manifest))
        before = copy.deepcopy(memory.state)
        alerts.prepare(memory, "news", "real", tmp_path, manifest)
        assert memory.state == before and not memory.writes
        assert len(alerts.load(tmp_path / "history.json")["items"]) == 134
        assert len(alerts.load(tmp_path / "replay.json")["candidates"]) == 14

    def test_test_mode_does_not_repair_incident(self, tmp_path, manifest):
        memory = FakeMemory(self.incident_state(manifest))
        before = copy.deepcopy(memory.state)
        alerts.prepare(memory, "news", "test", tmp_path, manifest)
        assert alerts.load(tmp_path / "replay.json") is None
        assert alerts.load(tmp_path / "history.json") == before
        assert not memory.writes


class TestGitHubCompareAndSwap:
    def test_conflict_re_reads_and_preserves_concurrent_updates(self):
        memory = alerts.Memory("owner/repo", "dummy", "news")
        initial = {"items": [], "extra": "preserved"}
        concurrent = {"items": [{"title_hash": "another-delivery"}], "extra": "newer"}
        memory.read = mock.Mock(side_effect=[(initial, "sha-1"), (concurrent, "sha-2")])
        memory.request = mock.Mock(side_effect=[HTTPError("url", 409, "conflict", {}, None), {}])

        def change(state):
            state["marker"] = True
            return state

        with mock.patch.object(alerts.time, "sleep"):
            result = memory.update(change)
        assert result["items"] == concurrent["items"]
        assert result["extra"] == "newer"
        payload = memory.request.call_args.args[2]
        assert payload["sha"] == "sha-2" and payload["branch"] == alerts.BRANCH
        assert json.loads(base64.b64decode(payload["content"])) == result
        assert memory.url.endswith("/contents/news/seen.json")  # Never rewrite branch tree/Reddit.

    def test_missing_history_fails_closed(self):
        memory = alerts.Memory("owner/repo", "dummy", "news")
        memory.request = mock.Mock(side_effect=HTTPError("url", 404, "missing", {}, None))
        with pytest.raises(HTTPError):
            memory.read()

    def test_malformed_history_fails_closed(self):
        memory = alerts.Memory("owner/repo", "dummy", "news")
        memory.request = mock.Mock(return_value={"sha": "sha", "content": base64.b64encode(b"{}").decode()})
        with pytest.raises(ValueError, match="malformed"):
            memory.read()

    def test_api_authorization_and_request_are_correct(self):
        memory = alerts.Memory("owner/repo", "dummy", "news")
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b'{"ok":true}'
        with mock.patch.object(alerts, "urlopen", return_value=response) as request:
            assert memory.request("GET", memory.url) == {"ok": True}
        assert request.call_args.args[0].get_header("Authorization") == "Bearer " + memory.token


class TestSmtp:
    @pytest.fixture
    def email(self):
        return {"subject": "Third Place News Alerts", "text": "actual report", "html": "<p>actual report</p>"}

    def test_smtp_success_uses_fixed_recipient_and_message_id(self, monkeypatch, email):
        monkeypatch.setenv("MAIL_USERNAME", "sender@example.net")
        monkeypatch.setenv("MAIL_PASSWORD", "dummy")
        client = mock.Mock()
        client.send_message.return_value = {}
        with mock.patch.object(alerts.smtplib, "SMTP_SSL", return_value=client) as smtp:
            alerts.smtp_send(email, "delivery-id")
        assert smtp.call_args.args[:2] == ("smtp.gmail.com", 465)
        message = client.send_message.call_args.args[0]
        assert str(message["To"]) == "segun@charlottethirdplaces.com"
        assert str(message["Message-ID"]) == "<delivery-id@charlottethirdplaces.com>"
        client.close.assert_called_once()

    def test_close_error_does_not_hide_smtp_acceptance(self, monkeypatch, email):
        monkeypatch.setenv("MAIL_USERNAME", "sender@example.net")
        monkeypatch.setenv("MAIL_PASSWORD", "dummy")
        client = mock.Mock()
        client.send_message.return_value = {}
        client.close.side_effect = OSError("close failed after DATA acknowledgement")
        with mock.patch.object(alerts.smtplib, "SMTP_SSL", return_value=client):
            alerts.smtp_send(email, "id")

    @pytest.mark.parametrize("error,retryable", [
        (smtplib.SMTPDataError(550, b"Rejected"), True),
        (smtplib.SMTPRecipientsRefused({"recipient": (550, b"rejected")}), True),
        (TimeoutError("lost acknowledgement"), False),
    ])
    def test_rejection_is_distinct_from_ambiguous_network_failure(self, monkeypatch, email, error, retryable):
        monkeypatch.setenv("MAIL_USERNAME", "sender@example.net")
        monkeypatch.setenv("MAIL_PASSWORD", "dummy")
        client = mock.Mock()
        client.send_message.side_effect = error
        with mock.patch.object(alerts.smtplib, "SMTP_SSL", return_value=client):
            with pytest.raises(alerts.NotAccepted if retryable else TimeoutError):
                alerts.smtp_send(email, "id")


class TestCompiledBoundary:
    @pytest.mark.parametrize("kind", ["news", "reddit"])
    def test_generated_workflow_has_trusted_boundary_and_no_premature_push(self, kind):
        source = (ROOT / f".github/workflows/{kind}-alerts.md").read_text()
        compiled = (ROOT / f".github/workflows/{kind}-alerts.lock.yml").read_text()
        assert "push_repo_memory:" not in compiled
        assert "  validate_alert_output:" in compiled
        assert "safeoutputs.jsonl" in compiled
        assert "Seal " in compiled and "Upload Immutable " in compiled
        assert compiled.index("Upload Immutable ") < compiled.index("Execute GitHub Copilot CLI")
        assert "ref: ${{ github.sha }}" in compiled
        assert "persist-credentials: false" in compiled
        assert "alert-inputs-${{ github.run_attempt }}" in compiled
        assert "scripts/alert_delivery.py deliver" in compiled
        assert "scripts/alert_delivery.py validate" in compiled
        assert "safeoutputs send_email_report . <" in source
        assert "Do not retry" in source or "Do not make live syntax probes" in source
        assert "After calling `send_email_report`" not in source
        delivery_job = compiled.split("\n  send_email_report:", 1)[1].split(
            "\n  validate_alert_output:", 1)[0]
        assert "contains(needs.agent.outputs.output_types, 'send_email_report')" in delivery_job
        assert "needs.agent.result == 'success'" in delivery_job
        assert "needs.detection.result == 'success' || needs.detection.result == 'skipped'" in delivery_job
        validation_job = compiled.split("\n  validate_alert_output:", 1)[1]
        assert "always() && !cancelled() && needs.agent.result == 'success'" in validation_job
        assert "contains(needs.agent.outputs.output_types" not in validation_job
        upload_step = compiled.split(f"- name: Upload Immutable {kind.title()} Candidates", 1)[1].split(
            "\n      - ", 1)[0]
        assert "if-no-files-found: error" in upload_step
        assert "overwrite: true" not in upload_step
        assert "path: ${{ runner.temp }}/alert-inputs/candidates.json" in upload_step

    def test_html_is_escaped_and_not_agent_authored(self, candidate):
        candidate["title"] = "Charlotte cafe <script>alert(1)</script> opens"
        email = alerts.render("news", "real", [candidate], selection_for(candidate))
        assert "<script>" not in email["html"]
        assert "&lt;script&gt;" in email["html"]

    def test_missing_artifacts_hard_fail_not_noop(self, monkeypatch, tmp_path):
        alerts.write(tmp_path / "event.json", {"inputs": {"mode": "real"}})
        monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
        monkeypatch.setenv("GITHUB_EVENT_PATH", str(tmp_path / "event.json"))
        monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
        monkeypatch.setenv("GH_TOKEN", "dummy")
        monkeypatch.setenv("GH_AW_AGENT_OUTPUT", str(tmp_path / "missing.json"))
        monkeypatch.setattr(sys, "argv", ["alert_delivery.py", "deliver", "--kind", "news", "--directory", str(tmp_path)])
        with mock.patch.object(alerts, "smtp_send") as sender:
            with pytest.raises(FileNotFoundError):
                alerts.main()
        sender.assert_not_called()
