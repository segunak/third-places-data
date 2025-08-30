-- Charlotte Third Places — Local Supabase validation suite
-- Purpose: verify schema readiness, seed sanity, and query performance signals.
-- Output: structured test rows with pass/fail and details, plus a few sample result sets.

-- =========================
-- 0) Environment checks
-- =========================
SELECT 'extensions:postgis_in_extensions' AS test,
             EXISTS (
                 SELECT 1 FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace
                 WHERE e.extname='postgis' AND n.nspname='extensions'
             ) AS pass,
             'postgis installed in extensions schema' AS details;

SELECT 'extensions:vector_pgtrgm_unaccent_in_extensions' AS test,
             (
                 SELECT bool_and(n.nspname='extensions')
                 FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace
                 WHERE e.extname IN ('vector','pg_trgm','unaccent','btree_gin')
             ) AS pass,
             'vector/pg_trgm/unaccent/btree_gin in extensions schema' AS details;

-- 1) Basic counts
SELECT 'seed:places_count' AS test,
             (SELECT count(*) FROM places) AS actual,
             5 AS expected,
             ((SELECT count(*) FROM places) = 5) AS pass;

SELECT 'seed:review_chunks_count' AS test,
             (SELECT count(*) FROM review_chunks) AS actual,
             6 AS expected,
             ((SELECT count(*) FROM review_chunks) = 6) AS pass;

-- 2) TSVs existence checks
SELECT 'schema:search_vectors_populated' AS test,
             COUNT(*) FILTER (WHERE search_tsv  IS NOT NULL AND search_tsv  <> ''::tsvector) AS populated_search_tsv,
             COUNT(*) FILTER (WHERE reviews_tsv IS NOT NULL AND reviews_tsv <> ''::tsvector) AS populated_reviews_tsv,
             COUNT(*) AS total_places,
             (COUNT(*) FILTER (WHERE search_tsv  IS NOT NULL AND search_tsv  <> ''::tsvector) = COUNT(*)) AS pass_search,
             (COUNT(*) FILTER (WHERE reviews_tsv IS NOT NULL AND reviews_tsv <> ''::tsvector) >= 1) AS pass_reviews
FROM places;

-- 3) FTS on places metadata
-- Expect Archive CLT due to Black-owned/book cafe vibe
SELECT 'fts:places_black_owned_includes_archive' AS test,
             EXISTS (
                 SELECT 1 FROM places
                 WHERE search_tsv @@ pg_catalog.plainto_tsquery('english','black owned')
                     AND place_name ILIKE 'Archive CLT%'
             ) AS pass,
             'Archive CLT should match black owned vibe' AS details;

SELECT 'fts:places_book_cafe_includes_archive' AS test,
             EXISTS (
                 SELECT 1 FROM places
                 WHERE search_tsv @@ pg_catalog.plainto_tsquery('english','book cafe')
                     AND place_name ILIKE 'Archive CLT%'
             ) AS pass,
             'Archive CLT should match book cafe vibe' AS details;

-- 4) FTS on reviews (quotes)
-- Expect Amélie’s / CHNO for “cinnamon rolls”, Qamaria for “booth”
SELECT 'fts:reviews_cinnamon_rolls_any_match' AS test,
             EXISTS (
                 SELECT 1 FROM places
                 WHERE reviews_tsv @@ pg_catalog.plainto_tsquery('english','cinnamon rolls')
             ) AS pass,
             'At least one place has review chunks mentioning cinnamon rolls' AS details;

SELECT 'fts:reviews_booth_any_match' AS test,
             EXISTS (
                 SELECT 1 FROM places
                 WHERE reviews_tsv @@ pg_catalog.plainto_tsquery('english','booth')
             ) AS pass,
             'At least one place has review chunks mentioning booth seating' AS details;

-- 5) Geospatial sanity — within ~8km of Uptown (35.2271,-80.8431)
SELECT 'geo:within_8km_of_uptown_exists' AS test,
             EXISTS (
                 SELECT 1 FROM places
                 WHERE extensions.ST_DWithin(geog, extensions.ST_SetSRID(extensions.ST_MakePoint(-80.8431,35.2271),4326)::extensions.geography, 8000)
             ) AS pass,
             'At least one place is within 8km of Uptown' AS details;

-- 6) Fuzzy find (pg_trgm)
SELECT 'trgm:amelie_similarity' AS test,
             EXISTS (
                 SELECT 1 FROM places
                 WHERE extensions.similarity(extensions.unaccent(lower(place_name)), extensions.unaccent(lower('Amelie'))) > 0.3
                    OR place_name ILIKE '%amelie%'
             ) AS pass,
             'Fuzzy search should match Amélie’s' AS details;

-- 7) JSONB-indexed filter — rating >= 4.8
SELECT 'jsonb:rating_filter_available' AS test,
             EXISTS (
                 SELECT 1 FROM places
                 WHERE (enriched_data->'details'->'raw_data'->>'rating')::float >= 4.8
             ) AS pass,
             'At least one place has rating >= 4.8' AS details;

-- 8) Working-hours helper
SELECT 'function:places_open_on_returns_rows' AS test,
             EXISTS (SELECT 1 FROM places_open_on('Sunday')) AS pass,
             'places_open_on("Sunday") returns at least 1 row' AS details;

-- 9) Materialized view refresh + query
REFRESH MATERIALIZED VIEW mv_place_review_chunks;
SELECT 'mv:chunk_search_works' AS test,
             EXISTS (
                 SELECT 1 FROM mv_place_review_chunks
                 WHERE chunk_tsv @@ pg_catalog.plainto_tsquery('english','booth')
             ) AS pass,
             'Materialized view search finds "booth"' AS details;

-- 10) Introspection — policies, triggers, indexes
SELECT 'rls:enabled_places' AS test,
             (SELECT relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='places') AS relrowsecurity,
             TRUE AS expected,
             ((SELECT relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='places') IS TRUE) AS pass;

SELECT 'rls:enabled_review_chunks' AS test,
             (SELECT relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='review_chunks') AS relrowsecurity,
             TRUE AS expected,
             ((SELECT relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='review_chunks') IS TRUE) AS pass;

SELECT 'schema:geog_column_type' AS test,
             (SELECT udt_schema||'.'||udt_name FROM information_schema.columns WHERE table_schema='public' AND table_name='places' AND column_name='geog') AS actual,
             'extensions.geography' AS expected,
             ((SELECT udt_schema||'.'||udt_name FROM information_schema.columns WHERE table_schema='public' AND table_name='places' AND column_name='geog')='extensions.geography') AS pass;

SELECT 'indexes:hnsw_embedding_places' AS test,
             EXISTS (
                 SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='places' AND indexdef ILIKE '%USING hnsw%embedding%'
             ) AS pass,
             'HNSW index exists on places.embedding' AS details;

SELECT 'indexes:hnsw_embedding_review_chunks' AS test,
             EXISTS (
                 SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='review_chunks' AND indexdef ILIKE '%USING hnsw%chunk_embedding%'
             ) AS pass,
             'HNSW index exists on review_chunks.chunk_embedding' AS details;

SELECT 'indexes:gin_search_tsv' AS test,
             EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='places' AND indexname='idx_places_search_tsv') AS pass,
             'GIN index on places.search_tsv' AS details;

SELECT 'indexes:gin_reviews_tsv' AS test,
             EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='places' AND indexname='idx_places_reviews_tsv') AS pass,
             'GIN index on places.reviews_tsv' AS details;

SELECT 'indexes:gist_geog' AS test,
             EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='places' AND indexname='idx_places_geog_gist') AS pass,
             'GiST index on places.geog' AS details;

SELECT 'indexes:mv_chunk_tsv' AS test,
             EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='mv_place_review_chunks' AND indexname='mv_idx_chunk_tsv') AS pass,
             'GIN index on mv_place_review_chunks.chunk_tsv' AS details;

-- Confirm index usage
EXPLAIN ANALYZE
SELECT google_maps_place_id
FROM places
WHERE search_tsv @@ pg_catalog.plainto_tsquery('english','book cafe');

EXPLAIN ANALYZE
SELECT google_maps_place_id
FROM places
WHERE extensions.ST_DWithin(geog, extensions.ST_SetSRID(extensions.ST_MakePoint(-80.8431,35.2271),4326)::extensions.geography, 8000);
