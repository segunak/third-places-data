# Charlotte Third Places Database Schema

Technical documentation for the PostgreSQL schema powering the Charlotte Third Places project. This schema is optimized for both traditional database operations and AI-powered semantic search capabilities.

## Architecture Overview

```txt
Airtable (curated data) + Google Places API (rich data) → PostgreSQL + pgvector → AI Chatbot
```

**Data Priority**: Airtable fields are authoritative for core place information (name, address, description). Google Places JSON provides supplementary data (reviews, hours, ratings, photos).

## Core Dependencies

- **PostgreSQL 15+** with extensions:
  - `vector` - High-dimensional embedding storage and similarity search
  - `pg_trgm` - Trigram matching for fuzzy text search
  - `btree_gin` - Composite indexing across data types
- **Supabase** - PostgreSQL hosting with built-in REST APIs
- **OpenAI/similar** - For generating 1536-dimensional embeddings

## Schema Design Principles

### 1. AI-First Architecture

- **Vector embeddings** enable semantic search beyond keyword matching
- **JSONB storage** preserves rich Google Places API responses without flattening
- **Full-text search vectors** provide traditional search capabilities
- **Review chunking** enables precise quote attribution for RAG (Retrieval Augmented Generation)

### 2. Performance Optimization

- **Specialized indexes** for different query patterns (IVFFLAT, GIN, BTREE)
- **Materialized views** for common AI query patterns
- **Automatic triggers** maintain search indexes without application logic

### 3. Data Source Hierarchy

- **Airtable data** is authoritative for core fields (name, address, website, description)
- **Google Places data** supplements with real-time info (hours, reviews, ratings)
- **Clear separation** prevents AI confusion between data sources

## Database Tables

### `places` Table

Primary entity representing a single third place venue.

**Key Design Decisions:**

- `google_maps_place_id` as PRIMARY KEY (stable Google identifier)
- `enriched_data JSONB` stores complete Google Places API responses
- `embedding VECTOR(1536)` stores OpenAI-compatible embeddings
- `reviews_tsv TSVECTOR` provides fast full-text search across all reviews

**Core Fields:**

```sql
google_maps_place_id    TEXT PRIMARY KEY    -- Stable Google identifier
airtable_record_id      TEXT NOT NULL       -- Link to curated data
place_name              TEXT NOT NULL       -- Authoritative name (Airtable)
latitude, longitude     DOUBLE PRECISION    -- GPS coordinates
address                 TEXT                -- Authoritative address (Airtable)
neighborhood            TEXT                -- Charlotte area designation
```

**Categorization:**

```sql
type                    TEXT[]              -- ["coffee_shop", "coworking", ...]
tags                    TEXT[]              -- ["quiet", "wifi", "outdoor_seating", ...]
size                    size_enum           -- Small/Medium/Large/Unsure
parking                 TEXT[]              -- ["street", "lot", "garage", ...]
```

**Amenities (ENUM-constrained):**

```sql
free_wi_fi              yes_no_unsure
purchase_required       yes_no_unsure  
has_cinnamon_rolls      yes_no_sometimes_unsure  -- Extended enum for seasonal items
operational             yes_no_unsure
```

**AI/Search Fields:**

```sql
enriched_data           JSONB               -- Complete Google Places API response
reviews_tsv             TSVECTOR            -- Auto-maintained from JSON reviews
embedding               VECTOR(1536)        -- Semantic similarity vector
```

### `review_chunks` Table

Enables precise quote attribution for AI responses using RAG pattern.

```sql
review_chunk_id         BIGSERIAL PRIMARY KEY
google_maps_place_id    TEXT REFERENCES places
review_id               TEXT                -- Original Google review ID
chunk_index             INT                 -- Position within original review
chunk_text              TEXT                -- 2-3 sentence chunk
chunk_tsv               TSVECTOR            -- Full-text search index
chunk_embedding         VECTOR(1536)        -- Semantic vector for chunk
```

**Purpose**: Instead of AI saying "reviews mention it's quiet," it can quote: "One customer noted: 'Perfect quiet corner for laptop work, minimal distractions.'"

## AI & Vector Search Implementation

### Embeddings Overview

**What**: 1536-dimensional numerical representations of text meaning
**Purpose**: Enable "semantic similarity" queries that understand context
**Example**: "cozy atmosphere" and "comfortable vibe" have similar embeddings

```sql
-- Find places semantically similar to a concept
SELECT place_name, neighborhood 
FROM places 
ORDER BY embedding <-> get_embedding('quiet study spot')::vector 
LIMIT 5;
```

### IVFFLAT Index Strategy

```sql
CREATE INDEX idx_places_embedding 
    ON places USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);
```

**How it works**:

- Groups embeddings into 100 clusters during index build
- Query searches relevant clusters only (approximate nearest neighbor)
- Trade-off: 95%+ accuracy with 10-100x speed improvement
- **Tuning**: Increase `lists` for larger datasets (rule of thumb: rows/1000)

### Full-Text Search (TSVECTOR)

```sql
-- Search across all reviews for specific terms
SELECT place_name, ts_rank(reviews_tsv, query) as relevance
FROM places, plainto_tsquery('english', 'quiet laptop work') query
WHERE reviews_tsv @@ query
ORDER BY relevance DESC;
```

**Features**:

- **Stemming**: "working" matches "work", "works"
- **Stop word removal**: Ignores "the", "and", "is"
- **Ranking**: More relevant matches score higher
- **Boolean queries**: `'coffee & wifi & !loud'`

### RAG (Retrieval Augmented Generation)

**Process**:

1. User query → semantic search finds relevant review chunks
2. AI reads specific passages with place context
3. AI generates response with precise citations

```sql
-- RAG query: Find review chunks about noise levels
SELECT place_name, chunk_text, chunk_index
FROM mv_place_review_chunks 
WHERE chunk_tsv @@ plainto_tsquery('quiet OR noise OR loud')
  AND neighborhood = 'South End'
ORDER BY ts_rank(chunk_tsv, plainto_tsquery('quiet OR noise OR loud')) DESC;
```

## Index Strategy

### Vector Indexes

```sql
-- Semantic similarity search
CREATE INDEX idx_places_embedding 
    ON places USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);

CREATE INDEX idx_review_chunks_embedding
    ON review_chunks USING ivfflat (chunk_embedding vector_l2_ops) WITH (lists = 100);
```

### Full-Text Search (GIN)

```sql
-- Fast text and array searches
CREATE INDEX idx_places_reviews_tsv ON places USING gin (reviews_tsv);
CREATE INDEX idx_places_type ON places USING gin (type);
CREATE INDEX idx_places_tags ON places USING gin (tags);
```

### JSONB Path Indexes

```sql
-- Fast queries into Google Places JSON structure
CREATE INDEX idx_places_google_rating 
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'rating'));
CREATE INDEX idx_places_working_hours
    ON places USING gin ((enriched_data->'details'->'raw_data'->'working_hours'));
```

### Fuzzy Search (Trigrams)

```sql
-- Typo-tolerant name search
CREATE INDEX idx_places_name_trgm
    ON places USING gin (lower(place_name) gin_trgm_ops);

-- Usage: WHERE lower(place_name) % 'starbks'  -- matches "Starbucks"
```

## Automatic Data Maintenance

### Triggers

**`refresh_reviews_tsv()`**: Automatically rebuilds full-text search index when Google Places data updates

```sql
-- Extracts all review_text fields from enriched_data JSON
-- Combines into single searchable TSVECTOR
-- Fires on INSERT/UPDATE of enriched_data
```

**`set_updated_at()`**: Standard timestamp maintenance

```sql
-- Updates updated_at column on any row change
-- Triggers on UPDATE of places table
```

### Views

**`places_enriched`**: Flattens JSONB into queryable columns

```sql
-- Exposes Google Places data as regular columns:
-- google_rating, review_count, working_hours, etc.
-- Makes AI queries simpler and more readable
```

**`mv_place_review_chunks`**: Materialized view for RAG queries

```sql
-- Pre-joins review_chunks with place context
-- Materialized = stored physically for speed
-- Refresh with: REFRESH MATERIALIZED VIEW mv_place_review_chunks;
```

## Data Overlap Management

### Authoritative Data Sources

| Field Type | Airtable (Authoritative) | Google Places (Reference) |
|-----------|-------------------------|---------------------------|
| **Identity** | `place_name`, `address` | `raw_data.name`, `raw_data.full_address` |
| **Contact** | `website` | `raw_data.site` |
| **Description** | Human-curated profile | `raw_data.description` (algorithmic) |
| **Unique to Google** | N/A | `rating`, `reviews`, `working_hours`, `popular_times` |

### Implementation Strategy

**For AI Queries**: Always use Airtable fields as primary data source. Use Google Places data for supplementary information only.

**Query Pattern**:

```sql
-- CORRECT: Use Airtable name as primary
SELECT place_name, google_rating, working_hours 
FROM places_enriched 
WHERE place_name ILIKE '%coffee%';

-- AVOID: Don't use Google name as primary source
-- SELECT google_name, google_rating...
```

## Performance Considerations

### Vector Index Maintenance

- **Build time**: IVFFLAT indexes require table scan during creation
- **Memory usage**: Higher `lists` parameter = more memory, better accuracy
- **Query planning**: Use `SET enable_seqscan = off;` to force index usage during testing

### JSONB Query Optimization

- **Path indexes**: Create indexes on frequently queried JSON paths
- **Expression indexes**: For computed values like `(data->>'rating')::FLOAT`
- **GIN indexes**: For existence queries (`WHERE data ? 'reviews'`)

### Materialized View Refresh

```sql
-- Manual refresh after bulk data changes
REFRESH MATERIALIZED VIEW mv_place_review_chunks;

-- Automated refresh via cron or application logic
-- Schedule based on data update frequency
```

## Development Workflow

### Schema Migrations

```bash
# Apply migrations
supabase db reset

# Generate new migration
supabase db diff --file new_migration_name

# Test locally
supabase start
```

### Testing Vector Queries

```sql
-- Test embedding similarity (requires actual embeddings)
SELECT place_name, embedding <-> '[0.1, 0.2, ...]'::vector as distance
FROM places 
ORDER BY distance 
LIMIT 5;

-- Test full-text search
SELECT place_name, ts_rank(reviews_tsv, query) as rank
FROM places, plainto_tsquery('coffee wifi quiet') query
WHERE reviews_tsv @@ query;
```

### Monitoring Performance

```sql
-- Check index usage
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read
FROM pg_stat_user_indexes 
ORDER BY idx_scan DESC;

-- Vector index stats
SELECT * FROM pg_stat_user_indexes WHERE indexname LIKE '%embedding%';
```

## API Integration

### Supabase Auto-Generated APIs

**REST Endpoints**:

```txt
GET  /rest/v1/places?select=*
POST /rest/v1/places
GET  /rest/v1/places_enriched?select=place_name,google_rating
```

**PostgREST Query Features**:

```txt
# Semantic search (custom function required)
GET /rest/v1/rpc/similarity_search?query_embedding=[...]&limit=10

# Full-text search
GET /rest/v1/places?reviews_tsv=fts.coffee.wifi

# Complex filters
GET /rest/v1/places?neighborhood=eq.South End&type=cs.{coffee_shop}
```

### Custom Functions for AI Features

```sql
-- Similarity search function
CREATE OR REPLACE FUNCTION similarity_search(
    query_embedding vector(1536),
    match_threshold float DEFAULT 0.8,
    match_count int DEFAULT 10
)
RETURNS TABLE (
    google_maps_place_id text,
    place_name text,
    similarity float
)
LANGUAGE sql STABLE
AS $$
    SELECT 
        p.google_maps_place_id,
        p.place_name,
        1 - (p.embedding <-> query_embedding) as similarity
    FROM places p
    WHERE 1 - (p.embedding <-> query_embedding) > match_threshold
    ORDER BY p.embedding <-> query_embedding
    LIMIT match_count;
$$;
```

## Security Configuration

### Row Level Security (RLS)

```sql
-- Enable RLS for public API access
ALTER TABLE places ENABLE ROW LEVEL SECURITY;
CREATE POLICY places_select_policy ON places FOR SELECT USING (true);

-- Read-only access to review chunks
ALTER TABLE review_chunks ENABLE ROW LEVEL SECURITY;  
CREATE POLICY chunks_select_policy ON review_chunks FOR SELECT USING (true);
```

### API Rate Limiting

Configure in Supabase dashboard:

- Anonymous requests: 100/hour
- Authenticated requests: 1000/hour
- Burst protection: 10 requests/second

## Troubleshooting

### Vector Search Issues

```sql
-- Check if embeddings exist
SELECT COUNT(*) FROM places WHERE embedding IS NOT NULL;

-- Verify index is being used
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM places ORDER BY embedding <-> '[0.1,0.2,...]'::vector LIMIT 5;
```

### Full-Text Search Issues

```sql
-- Check TSVECTOR content
SELECT place_name, reviews_tsv FROM places WHERE reviews_tsv IS NOT NULL LIMIT 5;

-- Test query parsing
SELECT plainto_tsquery('english', 'your search terms');
```

### JSONB Path Issues

```sql
-- Verify JSON structure
SELECT jsonb_pretty(enriched_data->'details'->'raw_data') FROM places LIMIT 1;

-- Test path expressions
SELECT enriched_data->'details'->'raw_data'->>'rating' FROM places WHERE enriched_data IS NOT NULL LIMIT 5;
```
