---
name: place-json-evidence
description: Extracts deterministic, cited evidence from data/places/charlotte Google Maps place JSON files. Use when researching Charlotte third places, joining Airtable Google Maps Place IDs to local JSON files, selecting review evidence, summarizing recency, or citing exact JSON fields.
---

# Charlotte Place JSON Evidence

Use this skill whenever you need local evidence from `data/places/charlotte/<Google Maps Place Id>.json` for Third Places Data.

This skill exists to make local JSON research repeatable. Use the deterministic extractor script [extract_place_json_evidence.py](./extract_place_json_evidence.py) through #tool:execute whenever you need evidence for one or more candidate places.

Do not parse place JSON with inline Python. Do not use Bash heredocs such as `python - <<'PY'`, PowerShell here-strings piped to `python -`, or `python -c` snippets. Those forms are shell-specific and can break across Windows PowerShell and Ubuntu Bash. Always run the checked-in extractor script file with command-line arguments.

## Input Contract

The input is one or more Google Maps Place IDs from Airtable field `Google Maps Place Id`.

Optional inputs:

- `--query`: Repeatable search terms or phrases from the user's request. Use broad and creative terms, including but not limited to seating, windows, fireplace, quiet, date, outlets, work, patio, stroller, dog, sensory, sunlight, menu, and any exact words the user used.
- `--max-reviews`: Maximum selected review evidence rows per place. Default: `12`.
- `--max-photos`: Maximum photo URLs per place. Default: `8`.
- `--include-raw-review-data`: Include compact `reviews.raw_data` place metadata when available.
- `--include-photo-raw-data`: Include compact `photos.raw_data` place metadata when available.

## Standard Command

Run from the repository root:

```powershell
python .github/skills/place-json-evidence/extract_place_json_evidence.py --place-id <GOOGLE_MAPS_PLACE_ID> --query "<term>" --query "<another term>"
```

For multiple place IDs:

```powershell
python .github/skills/place-json-evidence/extract_place_json_evidence.py --place-id <PLACE_ID_1> --place-id <PLACE_ID_2> --query "<term>" --max-reviews 8
```

If Python is unavailable in the current surface, manually follow the exact field contract below.

Do not replace the standard command with inline Python. If you need additional behavior, add it to the extractor script in a separate code-change task instead of improvising shell-specific snippets.

## Exact JSON Fields To Read

For each file, read these top-level fields:

- `place_id`
- `place_name`
- `data_source`
- `last_updated`
- `photos_provider_type` when present

Read these `details` fields:

- `details.place_name`
- `details.place_id`
- `details.google_maps_url`
- `details.website`
- `details.address`
- `details.description`
- `details.purchase_required`
- `details.parking`
- `details.latitude`
- `details.longitude`
- `details.error` and `details.message` when present

Read these `details.raw_data` fields when `details.raw_data` exists:

- `query`
- `name`
- `name_for_emails`
- `place_id`
- `google_id`
- `kgmid`
- `full_address`
- `borough`
- `street`
- `postal_code`
- `city`
- `us_state`
- `state`
- `country_code`
- `country`
- `latitude`
- `longitude`
- `time_zone`
- `site`
- `phone`
- `type`
- `category`
- `subtypes`
- `description`
- `typical_time_spent`
- `located_in`
- `reviews_tags`
- `rating`
- `reviews`
- `photos_count`
- `reviews_link`
- `reviews_id`
- `photo`
- `street_view`
- `working_hours`
- `working_hours_csv_compatible`
- `working_hours_old_format`
- `other_hours`
- `business_status`
- `about`
- `range`
- `prices`
- `reservation_links`
- `booking_appointment_link`
- `menu_link`
- `order_links`
- `owner_title`
- `owner_link`
- `location_link`
- `location_reviews_link`

Read these `reviews` fields:

- `reviews.place_id`
- `reviews.message`
- `reviews.reviews_data`
- `reviews.raw_data` when present, only for compact place-level metadata

For each item in `reviews.reviews_data`, read exactly these fields:

- `review_id`
- `review_text`
- `review_rating`
- `review_datetime_utc`
- `review_timestamp`
- `review_questions`
- `review_link`
- `review_img_urls`
- `review_img_url`
- `review_photo_ids`
- `review_likes`
- `author_title`
- `author_id`
- `author_reviews_count`
- `author_ratings_count`
- `owner_answer`
- `owner_answer_timestamp_datetime_utc`
- `reviews_id`
- `google_id`

Read these `photos` fields:

- `photos.place_id`
- `photos.message`
- `photos.photo_urls`
- `photos.last_refreshed` when present
- `photos.raw_data.photos_data` when present and relevant

Read these `photos.raw_data` place metadata fields when `photos.raw_data` exists:

- `name`
- `place_id`
- `full_address`
- `site`
- `phone`
- `type`
- `category`
- `subtypes`
- `reviews_tags`
- `rating`
- `reviews`
- `photos_count`
- `photo`
- `street_view`
- `working_hours`
- `business_status`
- `about`
- `menu_link`
- `owner_title`
- `location_link`
- `location_reviews_link`

## Evidence Selection Rules

- Deduplicate reviews by `review_id`.
- Use `review_id` only for deduplication and internal traceability. Do not present `review_id` as evidence to users.
- Use `review_datetime_utc` for recency. If it is missing or unparsable, fall back to `review_timestamp`. If neither can be parsed, mark date unknown.
- When query terms are supplied, rank reviews by direct term hits in `review_text`, `review_questions`, and `owner_answer`, then by recency.
- When no query term matches, return the newest reviews first.
- Include older reviews when they support stable physical traits, including but not limited to layout, windows, fireplaces, seating type, patio, decor, and outlets, but mark them as older.
- Treat `owner_answer` as owner context, not independent user evidence.
- Never cite duplicated text from `reviews.raw_data.reviews_data`; cite `reviews.reviews_data` instead.
- If `details.error` exists, return it and mark the file as a limited-evidence record.

## Output Contract

The script returns JSON with:

- `places[].place_id`
- `places[].file_path`
- `places[].found`
- `places[].top_level`
- `places[].details`
- `places[].details_raw_data`
- `places[].reviews_summary`
- `places[].selected_reviews`
- `places[].photos`
- `places[].photos_raw_data`
- `places[].warnings`

Use the returned JSON as the local evidence packet. Cite fields using their JSON paths, and cite selected reviews with a clickable `review_link`, `review_rating`, `review_datetime_utc`, and a direct quote. Use this format: `[Google review, <review_datetime_utc>, <review_rating> stars](<review_link>): "quoted text"`.