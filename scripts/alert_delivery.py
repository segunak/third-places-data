"""Trusted alert delivery boundary. Standard library only; never run inside the agent.

History lives in the existing memory branch. Contents API SHA preconditions serialize
each outbox, while updates to a single file preserve the other workflow's memory.
SMTP has no idempotency API: an interrupted/ambiguous send stays `sending` and blocks
automatic retries until an operator reconciles it. Never guess that it was delivered.
Successful receipts retain at most 90 days and 10,000 history entries/receipts.
A sealed history revision rejects stale snapshots even after their receipts expire.
Pending reports and the one-time recovery marker are never aged out.
"""

import argparse
import base64
import copy
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
import hashlib
import html
import json
import logging
import os
from pathlib import Path
import re
import smtplib
import ssl
import time
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
RECOVERY = ROOT / "data/alert-recovery/news-36350803135.json"
BRANCH = "memory/third-place-alerts"
RECOVERY_ID = "news-36350803135"
HISTORY_DAYS = 90
HISTORY_LIMIT = 10000
CATEGORIES = {
    "third-place": "An explicit third-place or third-space lead.",
    "openings-cafe": "A cafe opening, reopening, or new-location lead.",
    "openings-food": "A food venue with a third-place gathering signal.",
    "openings-bar": "A bar or brewery with a third-place gathering signal.",
    "openings-other": "A new or reopened community gathering place.",
    "closings": "A closure or change affecting a gathering place.",
    "reviews-spotlights": "A place spotlight worth evaluating for the directory.",
    "community-events": "A place-based gathering or creative-community lead.",
    "recommendations": "A recommendation or request revealing local third-place demand.",
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write(path: str | Path, value: Any) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def identity(item: dict[str, Any], kind: str) -> str:
    return item["title_hash" if kind == "news" else "id"]


def same(a: dict[str, Any], b: dict[str, Any], kind: str) -> bool:
    keys = ("url", "title_hash") if kind == "news" else ("id", "permalink", "text_hash")
    return any(a.get(key) and a.get(key) == b.get(key) for key in keys)


def trusted_mode(event_name: str, event: dict[str, Any]) -> str:
    if event_name == "schedule":
        return "real"
    if event_name != "workflow_dispatch":
        raise ValueError("Only scheduled or manually dispatched alert runs are supported")
    mode = event.get("inputs", {}).get("mode") or "real"
    if mode not in ("real", "test"):
        raise ValueError("Invalid trusted trigger mode")
    return mode


class Memory:
    def __init__(self, repository: str, token: str, kind: str) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("Invalid repository")
        if kind not in ("news", "reddit"):
            raise ValueError("Invalid alert kind")
        self.api = f"https://api.github.com/repos/{repository}"
        self.url = f"{self.api}/contents/{kind}/seen.json"
        self.token = token

    def request(
        self, method: str, url: str, body: dict[str, Any] | None = None,
        *, accept: str = "application/vnd.github+json",
    ) -> dict[str, Any]:
        request = Request(url, method=method, headers={
            "Authorization": "Bearer " + self.token,
            "Accept": accept,
            "X-GitHub-Api-Version": "2022-11-28",
        }, data=None if body is None else json.dumps(body).encode())
        with urlopen(request, timeout=60) as response:
            return json.load(response)

    def read(self) -> tuple[dict[str, Any], str]:
        result = self.request("GET", f"{self.url}?ref={quote(BRANCH, safe='')}")
        sha = result.get("sha") if isinstance(result, dict) else None
        if (not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha)
                or result.get("type") != "file"):
            raise ValueError("Missing or malformed history metadata")
        # Contents responses omit content above 1 MB. Read the immutable blob,
        # not the moving branch or a server-provided download_url, so the state
        # and the subsequent conditional PUT always refer to the same SHA.
        state = self.request("GET", f"{self.api}/git/blobs/{sha}",
                             accept="application/vnd.github.raw+json")
        if not isinstance(state, dict) or not isinstance(state.get("items"), list):
            raise ValueError("Missing or malformed history; refusing to assume empty history")
        return state, sha

    def update(
        self, change: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> dict[str, Any]:
        """Re-read and reapply a pure transition after each SHA/branch conflict."""
        for attempt in range(6):
            state, sha = self.read()
            result = change(copy.deepcopy(state))
            if result == state:
                return result
            try:
                self.request("PUT", self.url, {
                    "branch": BRANCH,
                    "sha": sha,
                    "message": "Record delivery-bound alert state",
                    "content": base64.b64encode(
                        (json.dumps(result, ensure_ascii=False, indent=2) + "\n").encode()
                    ).decode(),
                })
                return result
            except HTTPError as error:
                if error.code not in (409, 422) or attempt == 5:
                    raise
                time.sleep(attempt + 1)
        raise RuntimeError("Could not persist alert state")


def snapshot_name(memory: Memory, run_id: str, attempt: int, head_sha: str) -> str:
    """Resolve the immutable upload from the actual agent job, not this consumer.

    A failed-jobs-only rerun retains the earlier successful agent. A full rerun
    produces a new agent attempt and upload. Inspect all executions, never fall
    back to an older success when the latest producer failed or is incomplete.
    """
    if not re.fullmatch(r"[1-9][0-9]*", run_id) or attempt < 1:
        raise ValueError("Invalid workflow run")
    jobs = []
    page = 1
    while True:
        result = memory.request(
            "GET", f"{memory.api}/actions/runs/{run_id}/jobs?filter=all&per_page=100&page={page}",
        )
        batch, total = result.get("jobs"), result.get("total_count")
        if (not isinstance(batch, list) or not batch or type(total) is not int
                or total < 1 or any(not isinstance(job, dict) for job in batch)):
            raise ValueError("Missing or malformed producer job metadata")
        jobs.extend(batch)
        if len(jobs) >= total:
            break
        page += 1
    producers = [job for job in jobs if job.get("name") == "agent"]
    if not producers or any(type(job.get("run_attempt")) is not int
                            or not 1 <= job["run_attempt"] <= attempt for job in producers):
        raise ValueError("Missing or invalid agent producer attempt")
    producer_attempt = max(job["run_attempt"] for job in producers)
    latest = [job for job in producers if job["run_attempt"] == producer_attempt]
    if len(latest) != 1:
        raise ValueError("Ambiguous agent producer attempt")
    job = latest[0]
    if (job.get("status") != "completed" or job.get("conclusion") != "success"
            or job.get("run_id") != int(run_id) or job.get("head_sha") != head_sha):
        raise ValueError("Agent producer is not a successful job for this run and revision")
    return f"alert-inputs-{producer_attempt}"


def replay(
    state: dict[str, Any], kind: str, manifest: dict[str, Any],
) -> dict[str, Any] | None:
    delivery = state.get("_delivery", {})
    pending = delivery.get("pending")
    if pending:
        if pending["status"] != "prepared":
            raise ValueError("Ambiguous SMTP delivery: reconcile the sending outbox before retrying")
        return pending["report"]
    if kind == "news" and not delivery.get("recoveries", {}).get(RECOVERY_ID):
        return {
            "recovery": RECOVERY_ID,
            "candidates": manifest["items"],
            "selection": [
                {"id": item["title_hash"], "category": item["relevance_category"],
                 "evidence": item["title"]}
                for item in manifest["items"]
            ],
        }
    return None


def prepare(
    memory: Memory, kind: str, mode: str, directory: Path, manifest: dict[str, Any],
) -> None:
    """Read-only pre-agent snapshot. The subsequent artifact upload seals it."""
    state, _ = memory.read()
    report = replay(state, kind, manifest) if mode == "real" else None
    # The incident's false timestamps must not suppress recovery in local dedupe.
    if kind == "news" and report and report.get("recovery") == RECOVERY_ID:
        state["items"] = [
            item for item in state["items"]
            if not any(same(item, old, kind) and
                       item.get("last_notified_at") == manifest["false_notified_at"]
                       for old in manifest["items"])
        ]
    write(directory / "history.json", state)
    write(directory / "replay.json", report)


def validate_queue(output: dict[str, Any], raw: str) -> list[dict[str, str]] | None:
    """Validate both streams; ingestion may truncate duplicate requests to one."""
    if not isinstance(output, dict) or output.get("errors"):
        raise ValueError("Safe-output ingestion errors; no SMTP permitted")
    items = output.get("items")
    if not isinstance(items, list):
        raise ValueError("Missing safe-output items")
    requests = [json.loads(line) for line in raw.splitlines() if line.strip()]
    if len(requests) != 1 or len(items) != 1:
        raise ValueError("Exactly one final email request or noop is required, including raw output")
    item, request = items[0], requests[0]
    if not isinstance(request, dict) or request.get("type") != item.get("type"):
        raise ValueError("Raw and ingested output disagree")
    if item.get("type") == "noop":
        if not isinstance(item.get("message"), str) or not item["message"].strip():
            raise ValueError("A noop requires a reason")
        return None
    if item.get("type") != "send_email_report":
        raise ValueError("Unexpected safe output; refusing delivery")
    # Compare the structured selection even if ingestion added runtime metadata.
    if request.get("selection") != item.get("selection"):
        raise ValueError("Raw and ingested selections disagree")
    if any(key in item or key in request for key in ("subject", "text_body", "html_body", "mode")):
        raise ValueError("Agent-authored email bodies and modes are not accepted")
    # gh-aw sanitizes string inputs (including @mentions inside JSON). Transport
    # the structured JSON as base64 so a legitimate source quote is not altered.
    # This is encoding, not trust: IDs, categories and decoded evidence are all
    # validated against the immutable pre-agent snapshot below.
    selection = json.loads(base64.b64decode(item["selection"], validate=True))
    if not isinstance(selection, list) or not 1 <= len(selection) <= 20:
        raise ValueError("Select between 1 and 20 actual candidates")
    return selection


def validate_selection(
    selection: list[dict[str, str]] | None, snapshot: dict[str, Any], kind: str, mode: str,
) -> list[dict[str, Any]] | None:
    if snapshot.get("error") or snapshot.get("mode") != mode:
        raise ValueError("Collection failed or trigger/snapshot mode mismatch")
    candidates = snapshot["candidates"]
    by_id = {identity(item, kind): item for item in candidates}
    if len(by_id) != len(candidates):
        raise ValueError("Duplicate candidate identifiers")
    if selection is None:
        if snapshot.get("replay") or mode == "test":
            raise ValueError("Required replay/test report cannot be dropped as noop")
        return None
    seen = set()
    selected = []
    for entry in selection:
        if not isinstance(entry, dict) or set(entry) != {"id", "category", "evidence"}:
            raise ValueError("Selection requires only id, category and verbatim evidence")
        key = entry["id"]
        if key in seen or key not in by_id:
            raise ValueError("Duplicate or unknown selected candidate")
        seen.add(key)
        item = by_id[key]
        url = item.get("url" if kind == "news" else "permalink", "")
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username:
            raise ValueError("Candidate requires a public HTTPS source")
        if not item.get("title") or (kind == "news" and not item.get("source")):
            raise ValueError("Candidate missing substantive title/source")
        if mode == "real" and (
            item.get("source_kind") == "test" or
            parsed.hostname in ("example.com", "example.org", "localhost") or
            re.match(r"(?i)^(test[: ]|placeholder|synthetic)", item["title"])
        ):
            raise ValueError("Synthetic/placeholder content in a real run")
        if mode == "real" and kind == "news" and not snapshot.get("replay") and not item.get("location_evidence"):
            raise ValueError("News candidate lacks collected location evidence")
        category = entry["category"]
        evidence = entry["evidence"]
        fields = [item.get(field) or "" for field in
                  ("title", "description", "article_text_excerpt", "text")]
        if category not in CATEGORIES or not isinstance(evidence, str):
            raise ValueError("Invalid relevance category/evidence")
        if len(evidence.split()) < 3 or not any(evidence in field for field in fields):
            raise ValueError("Evidence must be a substantive verbatim candidate quote")
        if re.match(r"(?i)^(test body|placeholder|lorem ipsum|synthetic test)", evidence) and mode == "real":
            raise ValueError("Placeholder evidence")
        selected.append(copy.deepcopy(item))
    if snapshot.get("replay") and selection != snapshot["replay"]["selection"]:
        raise ValueError("Replay must include exactly the durable report, without omission or alteration")
    return selected


def render(
    kind: str, mode: str, candidates: list[dict[str, Any]],
    selection: list[dict[str, str]], recovery: str | None = None,
) -> dict[str, str]:
    prefix = "[TEST] " if mode == "test" else ""
    subject = f"{prefix}Third Place {kind.title()} Alerts"
    lines = [subject]
    if recovery:
        subject += " - Recovery 36350803135"
        lines += ["Recovery of 14 unsent leads from 2026-09-27; these are historical, not current-event notices.",
                  "Evidence below is the verified incident title metadata; re-check dates before attending events."]
    for item, entry in zip(candidates, selection):
        url = item.get("url") or item["permalink"]
        source = item.get("source") or "r/Charlotte"
        lines += ["", item["title"], f"Source: {source}", url,
                  f"Why it matters: {CATEGORIES[entry['category']]}",
                  f"Matched evidence: {entry['evidence']}"]
    text = "\n".join(lines)
    if len(text) > 20000:
        raise ValueError("Rendered report exceeds the email size limit")
    body = "<!doctype html><html><body><div style=\"white-space:pre-wrap\">" + html.escape(text) + "</div></body></html>"
    return {"subject": subject, "text": text, "html": body}


class NotAccepted(Exception):
    """SMTP explicitly did not accept the message; automatic retry is safe."""


def smtp_send(report: dict[str, str], delivery_id: str) -> None:
    message = EmailMessage()
    message["Subject"] = report["subject"]
    message["From"] = f"Charlotte Third Places Alerts <{os.environ['MAIL_USERNAME']}>"
    message["To"] = "segun@charlottethirdplaces.com"
    message["Message-ID"] = f"<{delivery_id}@charlottethirdplaces.com>"
    message.set_content(report["text"])
    message.add_alternative(report["html"], subtype="html")
    client = None
    try:
        try:
            client = smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=60, context=ssl.create_default_context())
            client.login(os.environ["MAIL_USERNAME"], os.environ["MAIL_PASSWORD"])
        except Exception as error:
            raise NotAccepted("SMTP connection/authentication failed before sending") from error
        try:
            refused = client.send_message(message)
            if refused:
                raise NotAccepted("SMTP refused the recipient")
        except (smtplib.SMTPRecipientsRefused, smtplib.SMTPSenderRefused, smtplib.SMTPDataError) as error:
            raise NotAccepted("SMTP explicitly rejected the message") from error
        # A disconnect during DATA is ambiguous and deliberately not retryable here.
    finally:
        if client is not None:
            try:
                client.close()  # No QUIT/close error may obscure a successful DATA acknowledgement.
            except OSError:
                pass


def retained_indices(
    records: list[dict[str, Any]], timestamp_key: str, sent_at: str,
) -> set[int]:
    """Keep recent records, then apply the count cap without changing their fields."""
    reference = datetime.fromisoformat(sent_at)
    cutoff = reference - timedelta(days=HISTORY_DAYS)
    dated = []
    for index, record in enumerate(records):
        value = record.get(timestamp_key) or record.get("first_seen_at")
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        except (AttributeError, TypeError, ValueError):
            # Do not infer that undated legacy history has expired. It still
            # participates in the count cap; later entries win timestamp ties.
            timestamp = reference
        if timestamp >= cutoff:
            dated.append((timestamp, index))
    return {index for _, index in sorted(dated, reverse=True)[:HISTORY_LIMIT]}


def deliver(
    memory: Memory, output: dict[str, Any], raw: str, snapshot: dict[str, Any],
    kind: str, mode: str, manifest: dict[str, Any],
    sender: Callable[[dict[str, str], str], None], run_id: str, validate_only: bool = False,
) -> str:
    selection = validate_queue(output, raw)
    candidates = validate_selection(selection, snapshot, kind, mode)
    if mode == "test":
        email = render(kind, mode, candidates, selection)
        if validate_only:
            return "test request validated"
        sender(email, f"test-{kind}-{run_id}")
        return "test sent; history unchanged"
    if validate_only and candidates is not None:
        render(kind, mode, candidates, selection, (snapshot.get("replay") or {}).get("recovery"))
        return "real request validated"
    state, _ = memory.read()
    required = replay(state, kind, manifest)
    if candidates is None:
        if required:
            raise ValueError("Outstanding recovery/outbox cannot be bypassed by noop")
        return "noop; history unchanged"
    recovery = (snapshot.get("replay") or {}).get("recovery")
    email = render(kind, mode, candidates, selection, recovery)
    report = {"candidates": candidates, "selection": selection, "recovery": recovery, "email": email}
    report_id = hashlib.sha256(json.dumps(report, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    if state.get("_delivery", {}).get("completed", {}).get(report_id):
        return "already delivered; no resend"
    if required and (selection != required["selection"] or candidates != required["candidates"]):
        raise ValueError("Outstanding recovery/outbox must be delivered before normal reports")
    if not required and recovery:
        raise ValueError("Stale recovery snapshot; recollect before sending")

    def stage(current: dict[str, Any]) -> dict[str, Any]:
        delivery = current.setdefault("_delivery", {})
        if delivery.get("completed", {}).get(report_id):
            return current
        pending = delivery.get("pending")
        if pending:
            if pending["id"] != report_id or pending["status"] != "prepared":
                raise ValueError("Another report is pending or delivery is ambiguous")
            return current
        # A bounded completed map cannot identify arbitrarily old report retries.
        # Only a snapshot of this history revision may create a new outbox.
        # Exact prepared outboxes above remain retryable across any time period.
        if snapshot.get("history_revision", 0) != delivery.get("revision", 0):
            raise ValueError("Stale history snapshot; recollect before sending")
        required_now = replay(current, kind, manifest)
        if required_now and (selection != required_now["selection"] or candidates != required_now["candidates"]):
            raise ValueError("Recovery changed while preparing report")
        if recovery:
            if not required_now:
                raise ValueError("Recovery already completed")
            # Remove only the 14 proven false notifications, not later legitimate history.
            current["items"] = [
                item for item in current["items"]
                if not any(same(item, old, kind) and
                           item.get("last_notified_at") == manifest["false_notified_at"]
                           for old in manifest["items"])
            ]
        elif any(same(item, previous, kind) for item in candidates for previous in current["items"]):
            raise ValueError("Selection includes already-notified items; recollect")
        delivery["pending"] = {"id": report_id, "status": "prepared", "report": report}
        return current

    state = memory.update(stage)
    if state["_delivery"].get("completed", {}).get(report_id):
        return "already delivered; no resend"

    def claim(current: dict[str, Any]) -> dict[str, Any]:
        pending = current["_delivery"]["pending"]
        if pending["id"] != report_id or pending["status"] != "prepared":
            raise ValueError("Concurrent or ambiguous delivery; refusing SMTP")
        pending.update(status="sending", run_id=run_id, started_at=now())
        return current

    memory.update(claim)
    try:
        sender(email, report_id)
    except NotAccepted:
        def retryable(current: dict[str, Any]) -> dict[str, Any]:
            pending = current["_delivery"]["pending"]
            if pending["id"] != report_id or pending.get("run_id") != run_id:
                raise ValueError("Delivery ownership changed")
            pending["status"] = "prepared"
            return current
        memory.update(retryable)
        raise
    sent_at = now()

    def receipt(current: dict[str, Any]) -> dict[str, Any]:
        delivery = current["_delivery"]
        if delivery.get("completed", {}).get(report_id):
            return current
        pending = delivery["pending"]
        if pending["id"] != report_id or pending.get("run_id") != run_id:
            raise ValueError("Delivery ownership changed after SMTP")
        for item, entry in zip(candidates, selection):
            matches = [old for old in current["items"] if same(item, old, kind)]
            if matches:
                record = matches[0]
            else:
                record = {}
                current["items"].append(record)
            keys = ("url", "source", "title", "title_hash") if kind == "news" else (
                "id", "permalink", "title", "text_hash", "source_query_family")
            record.update({key: item[key] for key in keys if key in item})
            record.setdefault("first_seen_at", manifest["false_notified_at"] if recovery else sent_at)
            record.update(last_notified_at=sent_at, relevance_category=entry["category"])
        delivery.setdefault("completed", {})[report_id] = {"sent_at": sent_at, "run_id": run_id}
        if recovery:
            delivery.setdefault("recoveries", {})[recovery] = {"sent_at": sent_at, "report_id": report_id}
        keep = retained_indices(current["items"], "last_notified_at", sent_at)
        current["items"] = [item for index, item in enumerate(current["items"]) if index in keep]
        completed = list(delivery["completed"].items())
        keep = retained_indices([value for _, value in completed], "sent_at", sent_at)
        delivery["completed"] = {
            key: value for index, (key, value) in enumerate(completed) if index in keep
        }
        delivery["revision"] = delivery.get("revision", 0) + 1
        del delivery["pending"]
        return current

    memory.update(receipt)
    return "SMTP accepted; exact report history recorded"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("prepare", "seal", "snapshot-name", "validate", "deliver"))
    parser.add_argument("--kind", required=True, choices=("news", "reddit"))
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    mode = trusted_mode(os.environ["GITHUB_EVENT_NAME"], load(os.environ["GITHUB_EVENT_PATH"]))
    manifest = load(RECOVERY)
    if args.command == "seal":
        snapshot = load(Path("/tmp/gh-aw/agent") / f"{args.kind}-candidates.json")
        if snapshot.get("error") or snapshot.get("mode") != mode:
            raise ValueError("Collection failed; no trusted snapshot will be uploaded")
        snapshot["replay"] = load(args.directory / "replay.json")
        history = load(args.directory / "history.json")
        snapshot["history_revision"] = history.get("_delivery", {}).get("revision", 0)
        write(args.directory / "candidates.json", snapshot)
        return
    memory = Memory(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"], args.kind)
    if args.command == "snapshot-name":
        name = snapshot_name(memory, os.environ["GITHUB_RUN_ID"],
                             int(os.environ["GITHUB_RUN_ATTEMPT"]), os.environ["GITHUB_SHA"])
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(f"name={name}\n")
        return
    if args.command == "prepare":
        prepare(memory, args.kind, mode, args.directory, manifest)
        state = load(args.directory / "history.json")
        write(Path("/tmp/gh-aw/repo-memory/third-place-alerts") / args.kind / "seen.json", state)
        return
    output_path = Path(os.environ["GH_AW_AGENT_OUTPUT"])
    # Missing raw/output/snapshot artifacts are hard failures, never successful noops.
    result = deliver(memory, load(output_path),
                     (output_path.parent / "safeoutputs.jsonl").read_text(encoding="utf-8"),
                     load(args.directory / "candidates.json"), args.kind, mode, manifest,
                     smtp_send, os.environ["GITHUB_RUN_ID"] + "-" + os.environ["GITHUB_RUN_ATTEMPT"],
                     validate_only=args.command == "validate")
    logging.info("%s", result)


if __name__ == "__main__":
    main()
