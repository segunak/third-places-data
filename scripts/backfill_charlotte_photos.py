import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


def is_valid_photo_url(url: str) -> bool:
    return isinstance(url, str) and url.startswith("http")


def parse_photo_date(date_str: str) -> datetime:
    try:
        return datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
    except (TypeError, ValueError):
        return datetime.min


def select_prioritized_photos(photos_data: List[Dict[str, Any]], max_photos: int = 30) -> List[str]:
    if not photos_data:
        return []

    photos_data.sort(key=lambda item: parse_photo_date(item.get("photo_date", "")), reverse=True)

    all_valid = [item for item in photos_data if isinstance(item, dict) and item.get("photo_url_big")]
    front, vibe, all_tag, other, tagless = [], [], [], [], []

    for photo in all_valid:
        tags = photo.get("photo_tags", [])
        if not isinstance(tags, list) or not tags:
            tagless.append(photo)
            continue
        if "front" in tags:
            front.append(photo)
        elif "vibe" in tags:
            vibe.append(photo)
        elif "all" in tags:
            all_tag.append(photo)
        elif "other" in tags:
            other.append(photo)
        else:
            tagless.append(photo)

    selected = []
    selected.extend(vibe[: min(len(vibe), max_photos)])
    remaining = max_photos - len(selected)

    selected.extend(front[: min(5, len(front), remaining)])
    remaining = max_photos - len(selected)

    selected.extend(all_tag[:remaining])
    remaining = max_photos - len(selected)

    selected.extend(other[:remaining])
    remaining = max_photos - len(selected)

    if remaining > 0:
        selected.extend(tagless[:remaining])

    unique_urls = []
    seen = set()
    for photo in selected:
        url = photo.get("photo_url_big")
        if not url or url in seen:
            continue
        unique_urls.append(url)
        seen.add(url)

    return unique_urls[:max_photos]


def extract_photo_records(raw_data: Any) -> Tuple[List[Dict[str, Any]], str]:
    if isinstance(raw_data, list) and raw_data:
        if isinstance(raw_data[0], dict) and "photo_url_big" in raw_data[0]:
            return raw_data, "direct_list"

    if isinstance(raw_data, dict):
        photos_data = raw_data.get("photos_data", [])
        if isinstance(photos_data, list) and photos_data:
            if isinstance(photos_data[0], dict) and "photo_url_big" in photos_data[0]:
                return photos_data, "nested_dict"

    return [], "unusable"


def process_file(file_path: Path, dry_run: bool) -> Dict[str, Any]:
    with file_path.open("r", encoding="utf-8") as fp:
        payload = json.load(fp)

    photos = payload.get("photos", {})
    if not isinstance(photos, dict):
        return {"status": "skipped_invalid", "file": str(file_path)}

    current_urls = photos.get("photo_urls", [])
    if not isinstance(current_urls, list):
        current_urls = []

    raw_data = photos.get("raw_data")
    photo_records, parse_method = extract_photo_records(raw_data)

    if not photo_records:
        if current_urls:
            return {
                "status": "preserved_existing",
                "file": str(file_path),
                "count": len(current_urls),
            }
        return {
            "status": "skipped_no_raw_data",
            "file": str(file_path),
        }

    valid_records = [record for record in photo_records if is_valid_photo_url(record.get("photo_url_big", ""))]
    selected_urls = select_prioritized_photos(valid_records, max_photos=30)

    if not selected_urls:
        if current_urls:
            return {
                "status": "preserved_existing",
                "file": str(file_path),
                "count": len(current_urls),
            }
        return {
            "status": "skipped_no_selectable_photos",
            "file": str(file_path),
            "parse_method": parse_method,
        }

    if selected_urls == current_urls:
        return {
            "status": "no_change",
            "file": str(file_path),
            "count": len(selected_urls),
            "parse_method": parse_method,
        }

    photos["photo_urls"] = selected_urls
    photos["message"] = (
        f"Backfilled from raw_data (parse_method={parse_method}) with no GPS-specific URL filtering; "
        f"selected {len(selected_urls)} photos"
    )
    photos["last_refreshed"] = datetime.now().isoformat()
    payload["photos"] = photos

    if not dry_run:
        with file_path.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp, indent=4, ensure_ascii=False)
            fp.write("\n")

    return {
        "status": "updated" if not dry_run else "would_update",
        "file": str(file_path),
        "count": len(selected_urls),
        "parse_method": parse_method,
    }


def summarize(results: List[Dict[str, Any]]) -> None:
    buckets = {}
    for item in results:
        buckets[item["status"]] = buckets.get(item["status"], 0) + 1

    print("Backfill summary:")
    for status in sorted(buckets.keys()):
        print(f"  {status}: {buckets[status]}")
    print(f"  total_files: {len(results)}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill data/places/<city> photo_urls from cached raw_data with current selection rules."
    )
    parser.add_argument("--city", default="charlotte", help="City folder under data/places (default: charlotte)")
    parser.add_argument("--workspace", default=".", help="Workspace root path (default: current directory)")
    parser.add_argument("--write", action="store_true", help="Apply file changes. Without this flag the script runs in dry-run mode.")
    args = parser.parse_args()

    dry_run = not args.write
    places_dir = Path(args.workspace).resolve() / "data" / "places" / args.city

    if not places_dir.exists() or not places_dir.is_dir():
        print(f"City directory not found: {places_dir}")
        return 1

    files = sorted(places_dir.glob("*.json"))
    if not files:
        print(f"No JSON files found in: {places_dir}")
        return 1

    print(f"Processing {len(files)} files in {places_dir} (dry_run={dry_run})")

    results = []
    for file_path in files:
        try:
            results.append(process_file(file_path, dry_run=dry_run))
        except Exception as exc:
            results.append({
                "status": "error",
                "file": str(file_path),
                "error": str(exc)
            })

    summarize(results)

    errors = [item for item in results if item["status"] == "error"]
    if errors:
        print("Errors:")
        for item in errors[:20]:
            print(f"  {item['file']}: {item['error']}")
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
