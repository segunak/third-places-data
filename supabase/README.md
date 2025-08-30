# Charlotte Third Places Database Schema

This is the "how it works" guide for the database that powers the Charlotte Third Places search and chatbot.

If you know basic software development but feel new to "AI databases," start here. The short version: we store places and small slices of review text, then build the right indexes so vague prompts like "quiet places to read near South End with free Wi‑Fi" come back fast and quotable.

Quick map for this repo:

- The schema is created by a single migration file: `supabase/migrations/20250829231528_create_core_schema.sql`
- You can read it top‑to‑bottom and match it to the sections below (extensions → tables → generated columns → indexes → triggers → views).

What makes this project special:

- Charlotte‑specific "third places" (not home/not work). The content is curated and enriched, so results feel local and opinionated.
- A chatbot that can answer vague prompts and back up claims with real quotes from reviews (not hand‑wavy summaries).
- Privacy: we store only the review text and the review’s date/time—no reviewer names, profile URLs, or other PII.

Start here: the mental model

- Two surfaces:
  - Places: one row per venue (`places`). Each place has an embedding for "vibe" search and a single weighted full‑text document for keyword search.
  - Review chunks: small 2–3 sentence passages (`review_chunks`) for precise citations. Each chunk has its own embedding for "find the exact quote."
- Three kinds of indexes do the heavy lifting:
  - Vector (HNSW) for semantic "similar meaning" searches
  - Full‑text (GIN) for keyword relevance and typo tolerance
  - Geospatial (GiST) for "near me" and radius filters
  - Plus time‑oriented (BRIN) and a few composite B‑tree indexes to make common filters cheap

Why this matters for Charlotte Third Places

- Users ask in natural language (vibes). Vector search gives us a shortlist of likely places.
- Users also care about concrete traits (Wi‑Fi, neighborhood, parking). Traditional indexes and full‑text search help us score/rank cleanly.
- Users want receipts. Review chunks let the chatbot cite specific sentences with dates, so answers feel trustworthy and current.

## Architecture Overview

```txt
Airtable (curated data) + Google Places API (enriched data) → PostgreSQL + pgvector → AI Chatbot
```

**Data Flow:**

1. Airtable contains curated place data (authoritative for core fields)
2. Google Places API provides enriched data (reviews, ratings, hours, photos)
3. PostgreSQL stores combined data with vector embeddings for semantic search
4. AI chatbot queries using RAG (Retrieval Augmented Generation) for precise citations

How to read the migration file (guided tour)

### Required extensions

- `vector` (for embeddings), `pg_trgm` (fuzzy text), `btree_gin` (mixed‑type indexes), `unaccent` (remove accents), and `postgis` (geospatial types + functions).
- Translation: these flip on Postgres features we rely on for AI‑ish and location‑ish queries.

### ENUM types

- We model "Yes/No/Unsure" patterns and a special "Yes/No/Sometimes/Unsure" for seasonal items (like cinnamon rolls) so queries are simple and consistent.

### `places` table (one row per venue)

- Core identity: `google_maps_place_id`, `place_name`, coordinates, neighborhood, tags, size, amenities.
- AI/search fields:
  - `embedding` (VECTOR(1536)) for "find similar vibe places."
  - `reviews_tsv` (TSVECTOR) auto‑maintained text index for keyword relevance.
  - `enriched_data` (JSONB) for additional details sourced from Google Places.
- Generated columns:
  - `geog` (geography point) built from lon/lat for fast radius queries.
  - `search_tsv` (TSVECTOR) combines name + tags + type + neighborhood + description into one weighted, searchable document.

### `review_chunks` table (quote‑friendly review slices)

- Stores small passages of review text, the place ID, a chunk index, the review timestamp, a text index (`chunk_tsv`), and an embedding (`chunk_embedding`).
- Why chunks? They’re perfect for "useful, quotable receipts" instead of dumping a whole review.
- Privacy: no reviewer names, profile links, or providers. Just text + when it was written.

### Indexes (performance backbone)

- Vector: HNSW on `places.embedding` and `review_chunks.chunk_embedding` (great recall/speed for changing data).
- Full‑text (GIN): `places.reviews_tsv`, `places.search_tsv`, and `review_chunks.chunk_tsv` for fast keyword search.
- Fuzzy name search: trigram on `lower(place_name)` (typo‑tolerant).
- Geospatial (GiST): on `places.geog` for "within X meters" using `ST_DWithin`.
- Time‑oriented: BRIN on created/updated times, and per‑place "most recent chunks" BTREE.
- Cheap pre‑filters: a few composite and partial indexes match how users typically ask (e.g., operational + featured, size + neighborhood).

### Triggers (auto‑maintenance)

- Touch `updated_at` automatically on updates.
- Keep `places.reviews_tsv` fresh from the latest review chunks—so place‑level text search always reflects current reviews without manual rebuilds.

### Database views

- `places_enriched`: flattens handy fields out of the big JSON, so you can SELECT them easily.
- `mv_place_review_chunks`: a materialized view that joins chunks with place context for faster RAG answers.

### Row‑level security

- Public read‑only policies so the site/app can read data safely by default.

The design goals, in plain English

- Fast answers to vague questions: vector gives a strong shortlist, then text and filters refine.
- Quotable, recent evidence: review chunks surface the exact lines and give them a timestamp.
- Local, relevant signals: neighborhood, size, amenities, and tags are all indexed to keep filters cheap.
- Safe by default: only review text + timestamp (no reviewer PII).

## Dependencies

- **PostgreSQL 15+** with extensions:
  - `vector` - High-dimensional embedding storage and indexing
  - `pg_trgm` - Fuzzy text matching and typo tolerance
  - `btree_gin` - Composite indexing support
- **Supabase** - Managed PostgreSQL with instant REST APIs
- **OpenAI API** - Text embedding generation (`text-embedding-ada-002`)

Note on embeddings

- We store 1536‑dimensional vectors. The values come from an embedding model (you can swap models later; keep the dimension consistent or migrate).
- Distance metric: we use L2 (Euclidean) right now (`vector_l2_ops`). If you normalize vectors, you can switch to cosine later.

## Core Tables

### `places`

Main venue entity storing both Airtable and Google Places data.

**Primary Key:** `google_maps_place_id` (stable Google identifier)

**Core Fields:**

```sql
google_maps_place_id    TEXT PRIMARY KEY    -- Stable Google identifier
record_id               TEXT NOT NULL       -- Airtable record ID
place_name              TEXT NOT NULL       -- Authoritative name
latitude, longitude     DOUBLE PRECISION    -- GPS coordinates
address                 TEXT                -- Authoritative address
neighborhood            TEXT                -- Charlotte area designation
```

**AI/Search Fields:**

```sql
enriched_data           JSONB               -- Complete Google Places API response
reviews_tsv             TSVECTOR            -- Auto-maintained full-text search index
embedding               VECTOR(1536)        -- OpenAI embedding for semantic search
```

**Categorization:**

```sql
type                    TEXT[]              -- ["Coffee Shop", "Bookstore", ...]
tags                    TEXT[]              -- ["Industrial Chic", "Quiet", ...]
size                    size_enum           -- Small/Medium/Large/Unsure
parking                 TEXT[]              -- ["Free", "Street", "Paid", ...]
```

**Amenities (ENUM-constrained):**

```sql
free_wi_fi              yes_no_unsure
purchase_required       yes_no_unsure
has_cinnamon_rolls      yes_no_sometimes_unsure
operational             yes_no_unsure
```

### `review_chunks`

Chunked review data for precise RAG citations.

```sql
review_chunk_id         BIGSERIAL PRIMARY KEY
google_maps_place_id    TEXT REFERENCES places
review_id               TEXT                -- Original Google review ID
chunk_index             INT                 -- Position within original review
chunk_text              TEXT                -- 2-3 sentence review fragment
chunk_tsv               TSVECTOR            -- Full-text search index
chunk_embedding         VECTOR(1536)        -- Semantic search embedding
```

**Purpose:** Enables AI to quote specific review passages rather than entire reviews.

How the two tables work together

- Typical flow for an answer:
  1) Use the user’s prompt to shortlist places (vector + simple filters + optionally full‑text).
  2) For the top N places, look up the most relevant/most recent review chunks.
  3) Return the places with a couple of short quotes that justify the recommendation.
  4) Optionally, re‑rank by recency, neighborhood proximity, or filters (Wi‑Fi, size).

## Index Strategy

### Vector Similarity (HNSW)

```sql
CREATE INDEX idx_places_embedding ON places USING hnsw (embedding vector_l2_ops);
CREATE INDEX idx_review_chunks_embedding ON review_chunks USING hnsw (chunk_embedding vector_l2_ops);
```

**Why HNSW over IVFFlat:** Better performance and robustness for changing data. Graph-based structure adapts automatically as new data is added.

Good defaults for this project

- HNSW works well as we continuously curate and enrich places.
- Keep vector indexes on both places and review chunks, because we search for "vibes" and "quotes."

### Full-Text Search (GIN)

```sql
CREATE INDEX idx_places_reviews_tsv ON places USING gin (reviews_tsv);
CREATE INDEX idx_places_type ON places USING gin (type);
CREATE INDEX idx_places_tags ON places USING gin (tags);
CREATE INDEX idx_places_parking ON places USING gin (parking);
```

### Exact Matching (BTREE)

```sql
CREATE INDEX idx_places_neighborhood ON places (neighborhood);
CREATE INDEX idx_places_operational ON places (operational);
CREATE INDEX idx_places_lat_long ON places (latitude, longitude);
```

### Fuzzy Matching (Trigram)

```sql
CREATE INDEX idx_places_name_trgm ON places USING gin (lower(place_name) gin_trgm_ops);
```

### JSONB Path Indexes

Key Google Places data fields indexed for fast queries:

```sql
CREATE INDEX idx_places_google_rating ON places USING btree ((enriched_data->'details'->'raw_data'->>'rating'));
CREATE INDEX idx_places_review_tags ON places USING gin ((enriched_data->'details'->'raw_data'->'reviews_tags'));
CREATE INDEX idx_places_working_hours ON places USING gin ((enriched_data->'details'->'raw_data'->'working_hours'));
-- ... (see migration file for complete list)
```

## Automated Maintenance

### Triggers

- **`trg_set_updated_at`**: Auto-updates `updated_at` timestamp on row changes
- **`trg_refresh_reviews_tsv`**: Rebuilds full-text search index when `enriched_data` changes
  - Also refreshed when review chunks change, so place‑level text search stays in sync with review text.

### Views

- **`places_enriched`**: Flattens Google Places JSON into queryable columns
- **`mv_place_review_chunks`**: Materialized view joining review chunks with place context

## Search Capabilities

### 1. Vector Similarity Search

```sql
SELECT place_name, neighborhood,
       embedding <-> get_embedding('quiet study spot')::vector as meaning_distance
FROM places 
ORDER BY meaning_distance
LIMIT 5;
```

### 2. Full-Text Search

```sql
SELECT place_name, ts_rank(reviews_tsv, query) as relevance
FROM places, plainto_tsquery('english', 'coffee wifi quiet') query
WHERE reviews_tsv @@ query
ORDER BY relevance DESC;
```

### 3. Hybrid Search

Combines vector similarity with traditional filters:

```sql
SELECT p.place_name
FROM places p
WHERE p.operational = 'Yes'
  AND p.neighborhood = 'South End'
  AND p.embedding <-> get_embedding('cozy coffee shop')::vector < 0.8
ORDER BY p.embedding <-> get_embedding('cozy coffee shop')::vector;
```

### 4. Quote Mining (review chunks)

Return quotable review passages for a place, favoring recent ones:

```sql
SELECT chunk_text, review_datetime_utc
FROM review_chunks
WHERE google_maps_place_id = $1
ORDER BY review_datetime_utc DESC, chunk_index
LIMIT 5;
```

Or search semantically for a quote:

```sql
SELECT rc.chunk_text,
       rc.review_datetime_utc,
       rc.chunk_embedding <-> get_embedding('quiet place to write with outlets')::vector AS meaning_distance
FROM review_chunks rc
JOIN places p USING (google_maps_place_id)
WHERE p.neighborhood = 'South End'
ORDER BY meaning_distance
LIMIT 5;
```

## Data Source Hierarchy

1. **Airtable fields are authoritative** for core data (name, address, website, description)
2. **Google Places data supplements** with real-time info (hours, reviews, ratings)
3. **Clear separation prevents AI confusion** between data sources

## Performance Notes

- **Vector indexes**: HNSW provides ~95% accuracy with 10-100x speed improvement over exact search
- **Materialized views**: Pre-computed for faster RAG queries
- **JSONB indexes**: Enable fast queries into Google Places API responses without schema changes
- **Composite indexes**: Optimized for common chatbot query patterns

Tuning hints (only if you need them)

- Text search: use `websearch_to_tsquery` for natural queries, and `ts_rank_cd` to score by density.
- Vector search: keep ANN first to shrink the candidate set, then apply filters and full‑text scoring.
- Geo search: use `ST_DWithin(geog, ST_MakePoint(lon,lat)::geography, meters)` to prefilter by distance.

## Development Setup

1. **Install Supabase CLI**: `npm install -g supabase`
2. **Start local instance**: `supabase start`
3. **Run migrations**: `supabase db reset`
4. **Verify extensions**: Check that `vector`, `pg_trgm`, and `btree_gin` are installed

## Production Considerations

- **Index build timing**: HNSW indexes can be built immediately, unlike IVFFlat
- **Memory usage**: Vector indexes require additional RAM - monitor usage
- **Embedding costs**: OpenAI API calls for generating embeddings - batch efficiently
- **Data freshness**: Implement proper sync strategies between Airtable and PostgreSQL

Privacy choices in this project

- We store only `review_text` (as `chunk_text`) and `review_datetime_utc` for quotes.
- We do not store reviewer names, profile links, or provider info.
- This gives us rich, recent signals without handling PII.

## Query Examples

See `/tests/` directory for more examples. If you’re skimming, the examples above are enough to understand how this database supports AI‑powered search for Charlotte Third Places.
