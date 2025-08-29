# Charlotte Third Places Database Schema

Technical documentation for the PostgreSQL schema powering the Charlotte Third Places AI-powered search system.

## Architecture Overview

```txt
Airtable (curated data) + Google Places API (enriched data) → PostgreSQL + pgvector → AI Chatbot
```

**Data Flow:**

1. Airtable contains curated place data (authoritative for core fields)
2. Google Places API provides enriched data (reviews, ratings, hours, photos)
3. PostgreSQL stores combined data with vector embeddings for semantic search
4. AI chatbot queries using RAG (Retrieval Augmented Generation) for precise citations

## Dependencies

- **PostgreSQL 15+** with extensions:
  - `vector` - High-dimensional embedding storage and indexing
  - `pg_trgm` - Fuzzy text matching and typo tolerance
  - `btree_gin` - Composite indexing support
- **Supabase** - Managed PostgreSQL with instant REST APIs
- **OpenAI API** - Text embedding generation (`text-embedding-ada-002`)

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

## Index Strategy

### Vector Similarity (HNSW)

```sql
CREATE INDEX idx_places_embedding ON places USING hnsw (embedding vector_l2_ops);
CREATE INDEX idx_review_chunks_embedding ON review_chunks USING hnsw (chunk_embedding vector_l2_ops);
```

**Why HNSW over IVFFlat:** Better performance and robustness for changing data. Graph-based structure adapts automatically as new data is added.

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

## Data Source Hierarchy

1. **Airtable fields are authoritative** for core data (name, address, website, description)
2. **Google Places data supplements** with real-time info (hours, reviews, ratings)
3. **Clear separation prevents AI confusion** between data sources

## Performance Notes

- **Vector indexes**: HNSW provides ~95% accuracy with 10-100x speed improvement over exact search
- **Materialized views**: Pre-computed for faster RAG queries
- **JSONB indexes**: Enable fast queries into Google Places API responses without schema changes
- **Composite indexes**: Optimized for common chatbot query patterns

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

## Query Examples

See `/tests/` directory for comprehensive query examples and performance benchmarks.
