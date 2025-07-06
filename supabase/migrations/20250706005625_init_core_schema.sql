/* ====================================================================/* =======================================================================
   PLACES TABLE – Main venue data
   ----------------------------------------------------------------- */
   CHARLOTTE THIRD PLACES DATABASE SCHEMA
   =======================================================================
   
   This migration creates a robust, AI-optimized PostgreSQL schema for 
   storing and searching Charlotte area "third places" (coffee shops, 
   libraries, coworking spaces, etc.).
   
   DESIGNED FOR:
   • Fast semantic search using AI embeddings (vector search)
   • Traditional full-text search with typo tolerance  
   • Efficient storage of both Airtable and Google Places data
   • AI chatbot queries with precise review quoting (RAG)
   • Future-friendly extensibility
   
   KEY FEATURES:
   • Vector embeddings for "find similar places" queries
   • JSONB storage for flexible Google Places API responses  
   • Chunked reviews for precise AI citations
   • Comprehensive indexes for sub-second search performance
   • Automatic maintenance via triggers
   • Ready-made views and helper functions
   
   FOR DETAILED EXPLANATIONS:
   See /supabase/README.md for plain-English explanations of all
   concepts, including what embeddings, RAG, IVFFLAT, GIN indexes,
   and materialized views actually mean and why we use them.
   
   ================================================================= */

/* =======================================================================
   REQUIRED EXTENSIONS
   ----------------------------------------------------------------- */

-- Vector extension: Core for AI semantic search capabilities
-- Enables storage and indexing of high-dimensional embeddings
CREATE EXTENSION IF NOT EXISTS vector;

-- Trigram extension: Enables fuzzy text matching and typo tolerance
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- BTree-GIN extension: Allows composite indexes mixing different types
CREATE EXTENSION IF NOT EXISTS btree_gin;

/* =======================================================================
   ENUM TYPES
   ----------------------------------------------------------------- 
   ALIGNED WITH AIRTABLE SCHEMA:
   • Most amenity fields use Yes/No/Unsure pattern consistently
   • Cinnamon rolls uses Yes/No/Sometimes/Unsure (special case for seasonal items)
   • Size includes 'Unsure' option for unclear cases  
   • No restrictions on Type/Parking arrays (too variable to constrain)
   ----------------------------------------------------------------- */
DO $$
BEGIN
    -- Standard tri-state for most amenity questions
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'yes_no_unsure') THEN
        CREATE TYPE yes_no_unsure AS ENUM ('Yes','No','Unsure');
    END IF;

    -- Extended enum for seasonal/variable items like cinnamon rolls
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'yes_no_sometimes_unsure') THEN
        CREATE TYPE yes_no_sometimes_unsure AS ENUM ('Yes','No','Sometimes','Unsure');
    END IF;

    -- Venue size categories
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'size_enum') THEN
        CREATE TYPE size_enum AS ENUM ('Small','Medium','Large','Unsure');
    END IF;
END$$;

/* =======================================================================
   2.  PLACES  – ONE ROW PER VENUE  (≈ your Airtable “base”)
   ----------------------------------------------------------------- */
CREATE TABLE places (
    /* ---------- identity & geo -------------------------------------- */
    google_maps_place_id    TEXT        PRIMARY KEY,
    record_id               TEXT,       NOT NULL     -- Airtable record ID
    place_name              TEXT        NOT NULL,
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,

    /* ---------- basic profile info ---------------------------------- */
    address                 TEXT,
    neighborhood            TEXT,
    -- TYPE: Array of place categories (coffee_shop, library, coworking, etc.)
    -- Can have multiple values since a place might be more than one type
    -- No ENUM constraint - values vary too much to restrict
    type                    TEXT[]      CHECK (array_ndims(type)=1),
    tags                    TEXT[]      CHECK (array_ndims(tags)=1),
    size                    size_enum,
    operational             yes_no_unsure,
    featured                BOOLEAN     DEFAULT FALSE,

    /* ---------- amenities (Yes/No/Unsure pattern) ------------------- */
    -- FREE_WI_FI: Does this place offer free WiFi?
    -- Values: Yes, No, Unsure (matches Airtable schema)
    free_wi_fi              yes_no_unsure,
    
    -- PURCHASE_REQUIRED: Do you need to buy something to sit/work here?
    -- Values: Yes, No, Unsure (matches Airtable schema)
    purchase_required       yes_no_unsure,
    
    -- PARKING: Array of parking options (Free, Paid, Street, Garage, etc.)
    -- No ENUM constraint - parking situations vary too much to restrict
    parking                 TEXT[]      CHECK (array_ndims(parking)=1),
    
    -- HAS_CINNAMON_ROLLS: Do they sell cinnamon rolls?
    -- Values: Yes, No, Sometimes, Unsure (matches Airtable schema)
    has_cinnamon_rolls      yes_no_sometimes_unsure,

    /* ---------- external links -------------------------------------- */
    website                 TEXT,
    google_maps_profile_url TEXT,
    apple_maps_profile_url  TEXT,
    facebook                TEXT,
    instagram               TEXT,
    twitter                 TEXT,
    linkedin                TEXT,
    tiktok                  TEXT,
    youtube                 TEXT,

    /* ---------- media ----------------------------------------------- */
    photos                  JSONB,      -- photo metadata from Google API
    curator_photos          JSONB,      -- manual uploads

    /* ---------- AI & search fields ---------------------------------- */
    -- Complete Google Places API response stored as searchable JSON
    enriched_data           JSONB,

    -- Auto-maintained full-text search vector from extracted Google reviews
    -- Built from individual review_text fields in enriched_data JSON
    reviews_tsv             TSVECTOR,

    -- 1536-dimensional embedding vector for semantic similarity search
    -- Generated from place description, reviews, and amenities
    -- Enables "find similar vibes" queries beyond keyword matching
    embedding               VECTOR(1536),

    /* ---------- sync/admin ------------------------------------------ */
    place_enhancements_link TEXT,
    has_data_file           yes_no_unsure,
    last_synced_airtable    TIMESTAMPTZ,
    last_synced_json        TIMESTAMPTZ,

    /* ---------- bookkeeping ----------------------------------------- */
    airtable_created_time       TIMESTAMPTZ,
    airtable_last_modified_time TIMESTAMPTZ,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);

/* =======================================================================
   REVIEW CHUNKS TABLE – For RAG (Retrieval Augmented Generation)
   -----------------------------------------------------------------
   Breaks long reviews into smaller chunks for precise AI citation.
   Instead of returning entire reviews, the chatbot can quote specific
   passages and reference exactly where information came from.
   
   Why separate table?
   • Vector indexes perform better on simpler tables
   • Chunking enables precise quote attribution vs vague summaries
   • Keeps embedding vectors focused and semantically coherent       */
CREATE TABLE review_chunks (
    review_chunk_id         BIGSERIAL PRIMARY KEY,
    google_maps_place_id    TEXT            REFERENCES places,
    review_id               TEXT,           -- original Google review_id
    chunk_index             INT,            -- 0,1,2 … within that review
    chunk_text              TEXT,
    chunk_tsv               TSVECTOR,
    chunk_embedding         VECTOR(1536)    -- same dimension as above
);

/* A composite index so you can “get me top-K similar vectors,
   but only from this place” if you want that later.                       */
CREATE INDEX idx_review_chunks_embedding
ON review_chunks
USING ivfflat (chunk_embedding vector_l2_ops) WITH (lists = 100);

/* =======================================================================
   INDEXES – Performance optimization for AI and traditional queries
   ----------------------------------------------------------------- */

/* VECTOR SIMILARITY INDEX
   IVFFLAT = Inverted File with Flat quantizer
   Groups similar embeddings into clusters for fast approximate search
   Lists=100 creates 100 clusters - tunable based on data size        */
CREATE INDEX idx_places_embedding
    ON places USING ivfflat (embedding vector_l2_ops) WITH (lists = 100);

/* FULL-TEXT SEARCH INDEXES
   GIN = Generalized Inverted Index - optimized for array/text search  */
CREATE INDEX idx_places_reviews_tsv          ON places USING gin (reviews_tsv);
CREATE INDEX idx_places_type                 ON places USING gin (type);
CREATE INDEX idx_places_tags                 ON places USING gin (tags);
CREATE INDEX idx_places_parking              ON places USING gin (parking);

/* STANDARD BTREE INDEXES for exact matching and sorting */
CREATE INDEX idx_places_neighborhood         ON places (neighborhood);
CREATE INDEX idx_places_operational          ON places (operational);
CREATE INDEX idx_places_featured             ON places (featured);
CREATE INDEX idx_places_lat_long             ON places (latitude, longitude);

/* TRIGRAM INDEX for fuzzy text matching - handles typos and partial matches */
CREATE INDEX idx_places_name_trgm
    ON places USING gin (lower(place_name) gin_trgm_ops);

/* COMPOSITE INDEXES for common chatbot query patterns */
CREATE INDEX idx_places_operational_featured  ON places (operational, featured);
CREATE INDEX idx_places_neighborhood_type     ON places USING gin (neighborhood, type);

/* JSONB PATH INDEXES for fast queries into Google Places data */
CREATE INDEX idx_places_google_rating 
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'rating'));
CREATE INDEX idx_places_price_range   
    ON places USING gin ((enriched_data->'details'->'raw_data'->>'range'));
CREATE INDEX idx_places_google_type   
    ON places USING gin ((enriched_data->'details'->'raw_data'->>'type'));

-- Popular times data for crowd patterns and busy periods
CREATE INDEX idx_places_popular_times
    ON places USING gin ((enriched_data->'details'->'raw_data'->'popular_times'));

-- Place description/summary text for semantic understanding
CREATE INDEX idx_places_description
    ON places USING gin (to_tsvector('english', enriched_data->'details'->'raw_data'->>'description'));

-- Review tags - highly valuable user-generated descriptors
-- Examples: "taro milk tea", "atmosphere", "games", "strawberry", "thai tea"
CREATE INDEX idx_places_review_tags
    ON places USING gin ((enriched_data->'details'->'raw_data'->'reviews_tags'));

-- Working hours for time-based queries
CREATE INDEX idx_places_working_hours
    ON places USING gin ((enriched_data->'details'->'raw_data'->'working_hours'));

-- About section - rich structured amenity data
CREATE INDEX idx_places_about_data
    ON places USING gin ((enriched_data->'details'->'raw_data'->'about'));

-- Business status (OPERATIONAL, CLOSED, etc.)
CREATE INDEX idx_places_business_status
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'business_status'));

-- Order links for e-commerce integration (DoorDash, UberEats, etc.)
CREATE INDEX idx_places_order_links
    ON places USING gin ((enriched_data->'details'->'raw_data'->'order_links'));

-- Contact information
CREATE INDEX idx_places_google_phone
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'phone'));
CREATE INDEX idx_places_google_website
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'site'));

-- Location details
CREATE INDEX idx_places_google_address
    ON places USING gin (to_tsvector('english', enriched_data->'details'->'raw_data'->>'full_address'));
CREATE INDEX idx_places_city
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'city'));
CREATE INDEX idx_places_state
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'us_state'));

-- Review metrics
CREATE INDEX idx_places_review_count
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'reviews'));

-- Category and subcategories
CREATE INDEX idx_places_category
    ON places USING gin (to_tsvector('english', enriched_data->'details'->'raw_data'->>'category'));
CREATE INDEX idx_places_subtypes
    ON places USING gin (to_tsvector('english', enriched_data->'details'->'raw_data'->>'subtypes'));

-- Time spent - useful for planning visits
CREATE INDEX idx_places_typical_time
    ON places USING gin (to_tsvector('english', enriched_data->'details'->'raw_data'->>'typical_time_spent'));

-- Booking and reservation links
CREATE INDEX idx_places_booking_links
    ON places USING gin ((enriched_data->'details'->'raw_data'->'reservation_links'));
CREATE INDEX idx_places_appointment_link
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'booking_appointment_link'));

-- Menu information
CREATE INDEX idx_places_menu_link
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'menu_link'));

-- Verification status for trust indicators
CREATE INDEX idx_places_verified
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'verified'));

-- Photos count for visual content richness
CREATE INDEX idx_places_photos_count
    ON places USING btree ((enriched_data->'details'->'raw_data'->>'photos_count'));

/* =======================================================================
   TRIGGERS – Auto-maintenance functions
   ----------------------------------------------------------------- */

/* Auto-update timestamp on record changes */
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN  NEW.updated_at = NOW();  RETURN NEW;  END; $$ LANGUAGE plpgsql;
CREATE TRIGGER trg_set_updated_at
    BEFORE UPDATE ON places
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

/* Auto-rebuild full-text search index when review content changes */
CREATE OR REPLACE FUNCTION refresh_reviews_tsv() RETURNS TRIGGER AS $$
DECLARE
    extracted TEXT;
BEGIN
    -- Extract all review texts from Google Places JSON
    IF NEW.enriched_data ? 'reviews' THEN
        SELECT string_agg(r->>'review_text', ' ')
          INTO extracted
          FROM jsonb_array_elements(NEW.enriched_data->'reviews'->'reviews_data') r
          WHERE r->>'review_text' IS NOT NULL;
    END IF;

    -- Build full-text search index from extracted reviews only
    NEW.reviews_tsv := to_tsvector('english', coalesce(extracted, ''));

    RETURN NEW;
END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_refresh_reviews_tsv
    BEFORE INSERT OR UPDATE OF enriched_data ON places
    FOR EACH ROW EXECUTE FUNCTION refresh_reviews_tsv();

/* =======================================================================
   VIEWS & HELPER FUNCTIONS – Query convenience layer
   ----------------------------------------------------------------- */

/* Flattened view extracting Google Places JSON into columns for easy access */
CREATE OR REPLACE VIEW places_enriched AS
SELECT
    p.*,
    -- Core Google Places metrics
    (p.enriched_data->'details'->'raw_data'->>'rating')::FLOAT          AS google_rating,
    (p.enriched_data->'details'->'raw_data'->>'reviews')::INT           AS review_count,
    p.enriched_data->'details'->'raw_data'->>'range'                    AS price_range,
    p.enriched_data->'details'->'raw_data'->>'type'                     AS google_place_type,
    p.enriched_data->'details'->'raw_data'->>'business_status'          AS business_status,
    (p.enriched_data->'details'->'raw_data'->>'verified')::BOOLEAN      AS google_verified,
    
    -- Contact and location details  
    p.enriched_data->'details'->'raw_data'->>'phone'                    AS google_phone,
    p.enriched_data->'details'->'raw_data'->>'site'                     AS google_website,
    p.enriched_data->'details'->'raw_data'->>'full_address'             AS google_address,
    p.enriched_data->'details'->'raw_data'->>'city'                     AS google_city,
    p.enriched_data->'details'->'raw_data'->>'us_state'                 AS google_state,
    
    -- Rich content for AI understanding
    p.enriched_data->'details'->'raw_data'->>'description'              AS google_description,
    p.enriched_data->'details'->'raw_data'->>'category'                 AS google_category,
    p.enriched_data->'details'->'raw_data'->>'subtypes'                 AS google_subtypes,
    p.enriched_data->'details'->'raw_data'->'reviews_tags'              AS google_review_tags,
    p.enriched_data->'details'->'raw_data'->>'typical_time_spent'       AS typical_time_spent,
    
    -- Operational data
    p.enriched_data->'details'->'raw_data'->'working_hours'             AS working_hours,
    p.enriched_data->'details'->'raw_data'->'popular_times'             AS popular_times,
    p.enriched_data->'details'->'raw_data'->'about'                     AS google_about,
    
    -- E-commerce and booking
    p.enriched_data->'details'->'raw_data'->'order_links'               AS order_links,
    p.enriched_data->'details'->'raw_data'->>'menu_link'                AS menu_link,
    p.enriched_data->'details'->'raw_data'->'reservation_links'         AS reservation_links,
    p.enriched_data->'details'->'raw_data'->>'booking_appointment_link' AS booking_link,
    
    -- Visual content indicators
    (p.enriched_data->'details'->'raw_data'->>'photos_count')::INT      AS photos_count,
    p.enriched_data->'details'->'raw_data'->>'photo'                    AS primary_photo_url
FROM   places p;

/* Materialized view for RAG queries - joins review chunks with place context
   Materialized = pre-computed and stored for faster chatbot response times */
CREATE MATERIALIZED VIEW mv_place_review_chunks AS
SELECT
    rc.*,
    pl.place_name,
    pl.neighborhood
FROM   review_chunks rc
JOIN   places        pl USING (google_maps_place_id);

CREATE INDEX mv_idx_chunk_tsv ON mv_place_review_chunks USING gin(chunk_tsv);

/* Helper function for time-based queries */
CREATE OR REPLACE FUNCTION places_open_on(day TEXT, time TEXT DEFAULT 'now')
RETURNS TABLE(place_id TEXT, place_name TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT google_maps_place_id,
           place_name::TEXT
    FROM   places_enriched
    WHERE  working_hours ->> day ILIKE '%'
       AND working_hours ->> day NOT ILIKE '%Closed%';
END; $$ LANGUAGE plpgsql STABLE;

/* =======================================================================
   ROW-LEVEL SECURITY – Public read-only access
   ----------------------------------------------------------------- */
ALTER TABLE places         ENABLE ROW LEVEL SECURITY;
CREATE POLICY places_rls   ON places
    FOR SELECT USING (true);

ALTER TABLE review_chunks  ENABLE ROW LEVEL SECURITY;
CREATE POLICY review_rls   ON review_chunks
    FOR SELECT USING (true);
