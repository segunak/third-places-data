#!/usr/bin/env python3
"""Extract deterministic evidence from Charlotte place JSON files."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DETAILS_RAW_KEYS = [
    "query",
    "name",
    "name_for_emails",
    "place_id",
    "google_id",
    "kgmid",
    "full_address",
    "borough",
    "street",
    "postal_code",
    "city",
    "us_state",
    "state",
    "country_code",
    "country",
    "latitude",
    "longitude",
    "time_zone",
    "site",
    "phone",
    "type",
    "category",
    "subtypes",
    "description",
    "typical_time_spent",
    "located_in",
    "reviews_tags",
    "rating",
    "reviews",
    "photos_count",
    "reviews_link",
    "reviews_id",
    "photo",
    "street_view",
    "working_hours",
    "working_hours_csv_compatible",
    "working_hours_old_format",
    "other_hours",
    "business_status",
    "about",
    "range",
    "prices",
    "reservation_links",
    "booking_appointment_link",
    "menu_link",
    "order_links",
    "owner_title",
    "owner_link",
    "location_link",
    "location_reviews_link",
]

DETAILS_KEYS = [
    "place_name",
    "place_id",
    "google_maps_url",
    "website",
    "address",
    "description",
    "purchase_required",
    "parking",
    "latitude",
    "longitude",
    "error",
    "message",
]

PHOTO_RAW_KEYS = [
    "name",
    "place_id",
    "full_address",
    "site",
    "phone",
    "type",
    "category",
    "subtypes",
    "reviews_tags",
    "rating",
    "reviews",
    "photos_count",
    "photo",
    "street_view",
    "working_hours",
    "business_status",
    "about",
    "menu_link",
    "owner_title",
    "location_link",
    "location_reviews_link",
]

REVIEW_KEYS = [
    "review_id",
    "review_text",
    "review_rating",
    "review_datetime_utc",
    "review_timestamp",
    "review_questions",
    "review_link",
    "review_img_urls",
    "review_img_url",
    "review_photo_ids",
    "review_likes",
    "author_title",
    "author_id",
    "author_reviews_count",
    "author_ratings_count",
    "owner_answer",
    "owner_answer_timestamp_datetime_utc",
    "reviews_id",
    "google_id",
]


def pick(mapping: Any, keys: list[str]) -> dict[str, Any]:
    if not isinstance(mapping, dict):
        return {}
    return {key: mapping.get(key) for key in keys if key in mapping}


def parse_review_datetime(review: dict[str, Any]) -> datetime | None:
    value = review.get("review_datetime_utc")
    if isinstance(value, str) and value.strip():
        for fmt in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass

    timestamp = review.get("review_timestamp")
    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return None


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def score_review(review: dict[str, Any], queries: list[str]) -> int:
    if not queries:
        return 0

    haystack = "\n".join(
        [
            normalize_text(review.get("review_text")),
            normalize_text(review.get("review_questions")),
            normalize_text(review.get("owner_answer")),
        ]
    ).lower()

    score = 0
    for query in queries:
        query = query.strip().lower()
        if not query:
            continue
        score += len(re.findall(re.escape(query), haystack)) * 3
        for token in re.findall(r"[a-z0-9]+", query):
            if len(token) >= 3:
                score += len(re.findall(rf"\b{re.escape(token)}\b", haystack))
    return score


def compact_review(review: dict[str, Any]) -> dict[str, Any]:
    compacted = pick(review, REVIEW_KEYS)
    parsed = parse_review_datetime(review)
    compacted["parsed_review_datetime_utc"] = parsed.isoformat() if parsed else None
    return compacted


def summarize_reviews(reviews: list[dict[str, Any]]) -> dict[str, Any]:
    years: Counter[str] = Counter()
    dated = 0
    for review in reviews:
        parsed = parse_review_datetime(review)
        if parsed:
            dated += 1
            years[str(parsed.year)] += 1
        else:
            years["unknown"] += 1

    return {
        "total_reviews_data_count": len(reviews),
        "dated_reviews_count": dated,
        "review_year_counts": dict(sorted(years.items(), reverse=True)),
    }


def unique_reviews(raw_reviews: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_reviews, list):
        return []

    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for index, review in enumerate(raw_reviews):
        if not isinstance(review, dict):
            continue
        review_id = str(review.get("review_id") or f"__missing_review_id_{index}")
        if review_id in seen:
            continue
        seen.add(review_id)
        unique.append(review)
    return unique


def select_reviews(reviews: list[dict[str, Any]], queries: list[str], max_reviews: int) -> list[dict[str, Any]]:
    scored = []
    for review in reviews:
        parsed = parse_review_datetime(review)
        timestamp = parsed.timestamp() if parsed else -1
        score = score_review(review, queries)
        scored.append((score, timestamp, review))

    if queries and any(score for score, _, _ in scored):
        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    else:
        scored.sort(key=lambda item: item[1], reverse=True)

    selected = []
    for score, _, review in scored[:max_reviews]:
        compacted = compact_review(review)
        compacted["query_match_score"] = score
        selected.append(compacted)
    return selected


def extract_place(path: Path, queries: list[str], max_reviews: int, max_photos: int, include_raw_review_data: bool, include_photo_raw_data: bool) -> dict[str, Any]:
    warnings: list[str] = []
    data = json.loads(path.read_text(encoding="utf-8"))

    details = data.get("details") if isinstance(data.get("details"), dict) else {}
    reviews_section = data.get("reviews") if isinstance(data.get("reviews"), dict) else {}
    photos_section = data.get("photos") if isinstance(data.get("photos"), dict) else {}

    if details.get("error"):
        warnings.append("details.error is present; this is a limited-evidence JSON file.")

    reviews = unique_reviews(reviews_section.get("reviews_data"))
    photo_urls = photos_section.get("photo_urls") if isinstance(photos_section.get("photo_urls"), list) else []

    details_raw = details.get("raw_data") if isinstance(details.get("raw_data"), dict) else {}
    reviews_raw = reviews_section.get("raw_data") if isinstance(reviews_section.get("raw_data"), dict) else {}
    photos_raw = photos_section.get("raw_data") if isinstance(photos_section.get("raw_data"), dict) else {}

    result = {
        "place_id": data.get("place_id"),
        "file_path": str(path.as_posix()),
        "found": True,
        "top_level": pick(data, ["place_id", "place_name", "data_source", "last_updated", "photos_provider_type"]),
        "details": pick(details, DETAILS_KEYS),
        "details_raw_data": pick(details_raw, DETAILS_RAW_KEYS),
        "reviews_summary": summarize_reviews(reviews),
        "selected_reviews": select_reviews(reviews, queries, max_reviews),
        "photos": {
            "place_id": photos_section.get("place_id"),
            "message": photos_section.get("message"),
            "last_refreshed": photos_section.get("last_refreshed"),
            "photo_urls_count": len(photo_urls),
            "photo_urls": photo_urls[:max_photos],
        },
        "photos_raw_data": pick(photos_raw, PHOTO_RAW_KEYS) if include_photo_raw_data else {},
        "warnings": warnings,
    }

    if include_raw_review_data:
        result["reviews_raw_data"] = pick(reviews_raw, DETAILS_RAW_KEYS)
    return result


def read_place_ids(args: argparse.Namespace) -> list[str]:
    place_ids = list(args.place_id or [])
    if args.place_ids_file:
        for line in Path(args.place_ids_file).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                place_ids.append(line)
    return list(dict.fromkeys(place_ids))


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Extract deterministic evidence from Charlotte place JSON files.")
    parser.add_argument("--root", default="data/places/charlotte", help="Directory containing place JSON files.")
    parser.add_argument("--place-id", action="append", help="Google Maps Place ID to extract. Repeat for multiple places.")
    parser.add_argument("--place-ids-file", help="Text file containing one Google Maps Place ID per line.")
    parser.add_argument("--query", action="append", default=[], help="Search term or phrase. Repeat for multiple terms.")
    parser.add_argument("--max-reviews", type=int, default=12, help="Maximum selected reviews per place.")
    parser.add_argument("--max-photos", type=int, default=8, help="Maximum photo URLs per place.")
    parser.add_argument("--include-raw-review-data", action="store_true", help="Include compact reviews.raw_data place metadata.")
    parser.add_argument("--include-photo-raw-data", action="store_true", help="Include compact photos.raw_data place metadata.")
    args = parser.parse_args()

    root = Path(args.root)
    place_ids = read_place_ids(args)
    if not place_ids:
        parser.error("Provide at least one --place-id or --place-ids-file.")

    output = {
        "root": str(root.as_posix()),
        "queries": args.query,
        "places": [],
    }

    for place_id in place_ids:
        path = root / f"{place_id}.json"
        if not path.exists():
            output["places"].append({"place_id": place_id, "file_path": str(path.as_posix()), "found": False, "warnings": ["JSON file not found."]})
            continue
        output["places"].append(
            extract_place(
                path=path,
                queries=args.query,
                max_reviews=args.max_reviews,
                max_photos=args.max_photos,
                include_raw_review_data=args.include_raw_review_data,
                include_photo_raw_data=args.include_photo_raw_data,
            )
        )

    print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())