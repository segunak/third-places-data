-- Charlotte Third Places — Local Supabase validation suite
-- Purpose: quick smoke tests after deploy/seed. Safe to re-run.

-- 1) Basic counts
SELECT count(*) AS places_count FROM places;
SELECT count(*) AS review_chunks_count FROM review_chunks;

-- 2) TSVs existence checks
SELECT place_name,
       (search_tsv  IS NOT NULL AND search_tsv  <> ''::tsvector) AS has_search_tsv,
       (reviews_tsv IS NOT NULL AND reviews_tsv <> ''::tsvector) AS has_reviews_tsv
FROM places
ORDER BY place_name;

-- 3) FTS on places metadata
-- Expect Archive CLT due to Black-owned/book cafe vibe
SELECT place_name,
       ts_rank(search_tsv, plainto_tsquery('english','black owned')) AS rank
FROM places
WHERE search_tsv @@ plainto_tsquery('english','black owned')
ORDER BY rank DESC, place_name;

SELECT place_name,
       ts_rank(search_tsv, plainto_tsquery('english','book cafe')) AS rank
FROM places
WHERE search_tsv @@ plainto_tsquery('english','book cafe')
ORDER BY rank DESC, place_name;

-- 4) FTS on reviews (quotes)
-- Expect Amélie’s / CHNO for “cinnamon rolls”, Qamaria for “booth”
SELECT place_name,
       ts_rank(reviews_tsv, plainto_tsquery('english','cinnamon rolls')) AS rank
FROM places
WHERE reviews_tsv @@ plainto_tsquery('english','cinnamon rolls')
ORDER BY rank DESC, place_name;

SELECT place_name,
       ts_rank(reviews_tsv, plainto_tsquery('english','booth')) AS rank
FROM places
WHERE reviews_tsv @@ plainto_tsquery('english','booth')
ORDER BY rank DESC, place_name;

-- 5) Geospatial sanity — within ~8km of Uptown (35.2271,-80.8431)
SELECT place_name,
       round(ST_Distance(geog, ST_SetSRID(ST_MakePoint(-80.8431,35.2271),4326)::geography)) AS meters
FROM places
WHERE ST_DWithin(geog, ST_SetSRID(ST_MakePoint(-80.8431,35.2271),4326)::geography, 8000)
ORDER BY meters ASC, place_name;

-- 6) Fuzzy find (pg_trgm)
SELECT place_name,
       similarity(place_name, 'Amelie') AS sim
FROM places
WHERE place_name ILIKE '%amelie%' OR similarity(place_name, 'Amelie') > 0.3
ORDER BY sim DESC NULLS LAST, place_name;

-- 7) JSONB-indexed filter — rating >= 4.8
SELECT place_name,
       (enriched_data->'details'->'raw_data'->>'rating')::float AS rating
FROM places
WHERE (enriched_data->'details'->'raw_data'->>'rating')::float >= 4.8
ORDER BY rating DESC, place_name;

-- 8) Working-hours helper
SELECT * FROM places_open_on('Sunday');

-- 9) Materialized view refresh + query
REFRESH MATERIALIZED VIEW mv_place_review_chunks;

SELECT place_name, chunk_text
FROM mv_place_review_chunks
WHERE chunk_tsv @@ plainto_tsquery('english','booth')
ORDER BY place_name, chunk_text;

-- 10) Introspection — policies, triggers, indexes
SELECT schemaname, tablename, policyname, roles, cmd, qual, with_check
FROM pg_policies
WHERE tablename IN ('places','review_chunks')
ORDER BY tablename, policyname;

SELECT event_object_table AS table, trigger_name, action_timing, event_manipulation
FROM information_schema.triggers
WHERE event_object_table IN ('places','review_chunks')
ORDER BY table, trigger_name;

SELECT tablename, indexname
FROM pg_indexes
WHERE schemaname='public' AND tablename IN ('places','review_chunks','mv_place_review_chunks')
ORDER BY tablename, indexname;

-- Confirm index usage
EXPLAIN ANALYZE
SELECT place_id
FROM places
WHERE search_tsv @@ plainto_tsquery('english','book cafe');
