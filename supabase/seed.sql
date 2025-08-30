-- Seed data for local testing
-- This file inserts a small, representative dataset to exercise:
-- - Enums (yes/no/unsure, yes/no/sometimes/unsure)
-- - Arrays (type, tags, parking)
-- - Geospatial generated column (geog from latitude/longitude)
-- - Triggers (compute_places_search_tsv, reviews_tsv sync from review_chunks)
-- - FTS on search_tsv and review text
-- - Views/functions (places_enriched, places_open_on)

BEGIN;

-- Clean up any prior seed rows for idempotent re-runs
DELETE FROM review_chunks
 WHERE google_maps_place_id IN (
	 'ChIJ6e1KBAAdVIgRBXdsuBE6sys', -- Qamaria Yemeni Coffee Co.
	 'ChIJWy8-QoM7TG8RttGA2Fc3yok', -- CHNO Coffee Co
	 'ChIJf5GCCCidVogRNg7nrE-mons', -- Sunflour Baking Company | Ballantyne
	 'ChIJvVxqVlSeVogRz786_h7uyiQ', -- Amélie’s Park Road
	 'ChIJzUFGL22hVogRXuG-SGJB45k'  -- Archive CLT
 );

DELETE FROM places
 WHERE google_maps_place_id IN (
	 'ChIJ6e1KBAAdVIgRBXdsuBE6sys',
	 'ChIJWy8-QoM7TG8RttGA2Fc3yok',
	 'ChIJf5GCCCidVogRNg7nrE-mons',
	 'ChIJvVxqVlSeVogRz786_h7uyiQ',
	 'ChIJzUFGL22hVogRXuG-SGJB45k'
);

-- 1) Qamaria Yemeni Coffee Co. (from Airtable CSV)
INSERT INTO places (
	google_maps_place_id, record_id, place_name,
	latitude, longitude,
	address, neighborhood, type, tags, size, operational, featured,
	free_wi_fi, purchase_required, parking, has_cinnamon_rolls,
	website, google_maps_profile_url, apple_maps_profile_url,
	tiktok, instagram, facebook, twitter, linkedin, youtube,
	photos, curator_photos,
	enriched_data,
	has_data_file, airtable_last_modified_time, airtable_created_time
) VALUES (
	'ChIJ6e1KBAAdVIgRBXdsuBE6sys', 'rec_qamaria', 'Qamaria Yemeni Coffee Co.',
	35.310945499999995, -80.7458656,
	'9325 Jw Clay Blvd Suite 223, Charlotte, NC 28262', 'University City',
	ARRAY['Coffee Shop','Café'], NULL, 'Medium'::size_enum, 'Yes'::yes_no_unsure, TRUE,
	'Yes'::yes_no_unsure, 'Yes'::yes_no_unsure, ARRAY['Free','Garage'], 'No'::yes_no_sometimes_unsure,
	'http://qamariacoffee.com/', 'https://maps.google.com/?cid=3148924412245079813', 'https://maps.apple.com/?ll=35.310945499999995,-80.7458656&q=Qamaria%20Yemeni%20Coffee%20Co.',
	'https://www.tiktok.com/@qamaricoffee_clt', 'https://www.instagram.com/qamariacoffee_clt', 'https://www.facebook.com/qamariacoffee/', NULL, NULL, NULL,
	jsonb_build_array(
		'https://lh3.googleusercontent.com/p/AF1QipMK42i8WYTT5uB7iHII7jB-2e5e7j9Hucsc_aU=w2048-h2048-k-no'
	), NULL,
	$$
	{
		"place_id": "ChIJ6e1KBAAdVIgRBXdsuBE6sys",
		"place_name": "Qamaria Yemeni Coffee Co.",
		"details": {
			"raw_data": {
				"rating": "4.8",
				"reviews": "120",
				"type": "Coffee shop",
				"description": "Authentic Yemeni coffee drinks, cozy booth-style seating, near UNC Charlotte.",
				"reviews_tags": ["booth seating","cheesecake","study","garage parking"],
				"working_hours": {"Monday": "7AM-8PM", "Sunday": "8AM-7PM"},
				"business_status": "OPERATIONAL",
				"verified": true,
				"full_address": "9325 Jw Clay Blvd Suite 223, Charlotte, NC 28262",
				"city": "Charlotte",
				"us_state": "North Carolina",
				"site": "http://qamariacoffee.com/",
				"category": "coffee shops",
				"subtypes": "Coffee shop, Cafe",
				"typical_time_spent": "People typically spend 1–2 hours here",
				"photos_count": 12,
				"phone": null
			}
		}
	}
	$$::jsonb,
	'Yes'::yes_no_unsure, to_timestamp('2025-08-29 13:05','YYYY-MM-DD HH24:MI'), to_timestamp('2025-08-23 08:43','YYYY-MM-DD HH24:MI')
)
ON CONFLICT (google_maps_place_id) DO NOTHING;

-- 2) CHNO Coffee Co (from Airtable CSV)
INSERT INTO places (
	google_maps_place_id, record_id, place_name,
	latitude, longitude,
	address, neighborhood, type, tags, size, operational, featured,
	free_wi_fi, purchase_required, parking, has_cinnamon_rolls,
	website, google_maps_profile_url, apple_maps_profile_url,
	tiktok, instagram, facebook,
	photos,
	enriched_data,
	has_data_file, airtable_last_modified_time, airtable_created_time
) VALUES (
	'ChIJWy8-QoM7TG8RttGA2Fc3yok', 'rec_chno', 'CHNO Coffee Co',
	35.212136799999996, -80.8013805,
	'1110 Morningside Dr Suite E, Charlotte, NC 28205', 'Plaza Midwood',
	ARRAY['Coffee Shop'], NULL, 'Small'::size_enum, 'Yes'::yes_no_unsure, TRUE,
	'Yes'::yes_no_unsure, 'Yes'::yes_no_unsure, ARRAY['Free'], 'Sometimes'::yes_no_sometimes_unsure,
	'https://chnocoffeeco.square.site/', 'https://maps.google.com/?cid=9928809178925683126', 'https://maps.apple.com/?ll=35.212136799999996,-80.8013805&q=CHNO%20Coffee%20Co',
	'https://www.tiktok.com/@chnocoffee.co', 'https://www.instagram.com/Chnocoffee.co/', 'https://www.facebook.com/chnocoffee.co/',
	jsonb_build_array('https://lh3.googleusercontent.com/p/AF1QipM0C0itAnAc8KmHPG3EOtopKyGCSJswNMjJ-zpP=w2048-h2048-k-no'),
	$$
	{
		"place_id": "ChIJWy8-QoM7TG8RttGA2Fc3yok",
		"place_name": "CHNO Coffee Co",
		"details": {
			"raw_data": {
				"rating": "4.9",
				"reviews": "85",
				"type": "Coffee shop",
				"description": "Cozy bookstore-meets-coffee-shop with herbal tea and Honeybear pastries.",
				"reviews_tags": ["cinnamon rolls","herbal tea","cozy"],
				"working_hours": {"Monday": "7AM-6PM"},
				"business_status": "OPERATIONAL",
				"verified": true,
				"full_address": "1110 Morningside Dr Suite E, Charlotte, NC 28205",
				"city": "Charlotte",
				"us_state": "North Carolina",
				"site": "https://chnocoffeeco.square.site/",
				"category": "coffee shops",
				"subtypes": "Coffee shop, Cafe",
				"typical_time_spent": "People typically spend 1–2 hours here",
				"photos_count": 8
			}
		}
	}
	$$::jsonb,
	'Yes'::yes_no_unsure, to_timestamp('2025-08-12 08:39','YYYY-MM-DD HH24:MI'), to_timestamp('2025-08-12 08:39','YYYY-MM-DD HH24:MI')
)
ON CONFLICT (google_maps_place_id) DO NOTHING;

-- 3) Sunflour Baking Company | Ballantyne (from Outscraper JSON)
INSERT INTO places (
	google_maps_place_id, record_id, place_name,
	latitude, longitude,
	address, neighborhood, type, tags, size, operational, featured,
	free_wi_fi, purchase_required, parking, has_cinnamon_rolls,
	website, google_maps_profile_url, apple_maps_profile_url,
	tiktok, instagram,
	enriched_data,
	has_data_file, airtable_last_modified_time, airtable_created_time
) VALUES (
	'ChIJf5GCCCidVogRNg7nrE-mons', 'rec_sunflour_ballantyne', 'Sunflour Baking Company | Ballantyne',
	35.0559715, -80.8539797,
	'14021 Conlan Cir B-9, Charlotte, NC 28277', 'Ballantyne',
	ARRAY['Bakery','Café'], NULL, 'Medium'::size_enum, 'Yes'::yes_no_unsure, FALSE,
	'Yes'::yes_no_unsure, 'Yes'::yes_no_unsure, ARRAY['Free','Parking lot'], 'Yes'::yes_no_sometimes_unsure,
	'https://sunflourbakingcompany.com/pages/ballantyne-bakery?utm_source=google', 'https://maps.google.com/?cid=8908865874025713206', 'https://maps.apple.com/?ll=35.0559715,-80.8539797&q=Sunflour%20Baking%20Company%20%7C%20Ballantyne',
	NULL, NULL,
	$$
	{
		"place_id": "ChIJf5GCCCidVogRNg7nrE-mons",
		"place_name": "Sunflour Baking Company | Ballantyne",
		"details": {
			"raw_data": {
				"rating": "4.3",
				"reviews": "518",
				"type": "Bakery",
				"description": "Sunflour Baking Company offers fresh, from-scratch pastries and gourmet coffee drinks in a welcoming cafe setting. Their convenient location near Ballantyne Business Park provides a perfect spot to relax, meet with colleagues, or enjoy a delicious breakfast or lunch. They also cater and offer wedding cakes.",
				"reviews_tags": ["muffins","vegan","gluten free","latte","cinnamon roll","scones","espresso"],
				"working_hours": {"Monday": "7AM-4PM", "Sunday": "7AM-4PM"},
				"business_status": "OPERATIONAL",
				"verified": true,
				"full_address": "14021 Conlan Cir B-9, Charlotte, NC 28277, United States",
				"city": "Charlotte",
				"us_state": "North Carolina",
				"site": "https://sunflourbakingcompany.com/pages/ballantyne-bakery?utm_source=google",
				"category": "Bakery",
				"subtypes": "Bakery, Breakfast restaurant, Brunch restaurant, Cafe, Caterer, Coffee shop, Dessert shop, Lunch restaurant, Pastry shop, Wedding bakery",
				"typical_time_spent": "People typically spend up to 45 min here",
				"photos_count": 410,
				"phone": "+1 980-237-8881"
			}
		}
	}
	$$::jsonb,
	'Yes'::yes_no_unsure, to_timestamp('2025-05-26 15:41','YYYY-MM-DD HH24:MI'), to_timestamp('2025-04-18 17:40','YYYY-MM-DD HH24:MI')
)
ON CONFLICT (google_maps_place_id) DO NOTHING;

-- 4) Amélie’s French Bakery & Café | Park Road (from Airtable CSV)
INSERT INTO places (
	google_maps_place_id, record_id, place_name,
	latitude, longitude,
	address, neighborhood, type, tags, size, operational, featured,
	free_wi_fi, purchase_required, parking, has_cinnamon_rolls,
	website, google_maps_profile_url, apple_maps_profile_url,
	tiktok, instagram, twitter, linkedin,
	enriched_data,
	has_data_file, airtable_last_modified_time, airtable_created_time
) VALUES (
	'ChIJvVxqVlSeVogRz786_h7uyiQ', 'rec_amelies_pr', 'Amélie’s French Bakery & Café | Park Road',
	35.1733556, -80.8473461,
	'524 Brandywine Rd, Charlotte, NC 28209', 'Montford',
	ARRAY['Bakery','Café'], ARRAY['Eclectic','Charming'], 'Large'::size_enum, 'Yes'::yes_no_unsure, FALSE,
	'Yes'::yes_no_unsure, 'Yes'::yes_no_unsure, ARRAY['Free'], 'Yes'::yes_no_sometimes_unsure,
	'http://www.ameliesfrenchbakery.com/', 'https://maps.google.com/?cid=2651193147542650831', 'https://maps.apple.com/?ll=35.1733556,-80.8473461&q=Am%C3%A9lie%E2%80%99s%20French%20Bakery%20%26%20Caf%C3%A9%20%7C%20Park%20Road',
	'https://www.tiktok.com/@ameliesfrenchbakery', 'https://www.instagram.com/ameliesfrenchbakery/', 'https://x.com/AmeliesBakery', 'https://www.linkedin.com/company/amelie-s-french-bakery',
	$$
	{
		"place_id": "ChIJvVxqVlSeVogRz786_h7uyiQ",
		"place_name": "Amélie’s French Bakery & Café | Park Road",
		"details": {
			"raw_data": {
				"rating": "4.6",
				"reviews": "2200",
				"type": "Cafe",
				"description": "Large, popular gathering place with great outlet access and pastries.",
				"reviews_tags": ["outlets","cinnamon rolls","pastries"],
				"working_hours": {"Monday": "7AM-10PM"},
				"business_status": "OPERATIONAL",
				"verified": true,
				"full_address": "524 Brandywine Rd, Charlotte, NC 28209",
				"city": "Charlotte",
				"us_state": "North Carolina",
				"site": "http://www.ameliesfrenchbakery.com/",
				"category": "cafes",
				"subtypes": "Bakery, Cafe",
				"typical_time_spent": "People typically spend 2–3 hours here",
				"photos_count": 30
			}
		}
	}
	$$::jsonb,
	'Yes'::yes_no_unsure, to_timestamp('2025-06-20 20:16','YYYY-MM-DD HH24:MI'), to_timestamp('2022-11-05 22:30','YYYY-MM-DD HH24:MI')
)
ON CONFLICT (google_maps_place_id) DO NOTHING;

-- 5) Archive CLT (from Outscraper JSON)
INSERT INTO places (
	google_maps_place_id, record_id, place_name,
	latitude, longitude,
	address, neighborhood, type, tags, size, operational, featured,
	free_wi_fi, purchase_required, parking, has_cinnamon_rolls,
	website, google_maps_profile_url,
	enriched_data,
	has_data_file
) VALUES (
	'ChIJzUFGL22hVogRXuG-SGJB45k', 'rec_archive_clt', 'Archive CLT',
	35.2626266, -80.85552969999999,
	'2023 Beatties Ford Rd Suite D, Charlotte, NC 28216', 'Washington Heights',
	ARRAY['Coffee shop'], ARRAY['WiFi','Black-owned','Book cafe'], 'Small'::size_enum, 'Yes'::yes_no_unsure, FALSE,
	'Yes'::yes_no_unsure, 'Yes'::yes_no_unsure, ARRAY['Free','Street','Parking lot'], 'Unsure'::yes_no_sometimes_unsure,
	'http://archiveclt.com/', 'https://maps.google.com/?cid=11088778597899362654',
	$$
	{
		"place_id": "ChIJzUFGL22hVogRXuG-SGJB45k",
		"place_name": "Archive CLT",
		"details": {
			"raw_data": {
				"rating": "5",
				"reviews": "185",
				"type": "Coffee shop",
				"description": "Archive CLT is a Black owned book cafe in Charlotte NC. We offer local roasted coffee and espresso along with your favorite nostalgic books, magazines and relics.",
				"working_hours": {"Monday": "8AM-6PM", "Sunday": "9AM-4PM"},
				"business_status": "OPERATIONAL",
				"verified": true,
				"full_address": "2023 Beatties Ford Rd Suite D, Charlotte, NC 28216, United States",
				"city": "Charlotte",
				"us_state": "North Carolina",
				"site": "http://archiveclt.com/",
				"category": "coffee shops",
				"subtypes": "Coffee shop",
				"photos_count": 135,
				"phone": "+1 980-349-5595",
				"reviews_tags": ["atmosphere","latte","space","culture","magazines","black owned","muffin","memorabilia","gem"]
			}
		}
	}
	$$::jsonb,
	'Yes'::yes_no_unsure
)
ON CONFLICT (google_maps_place_id) DO NOTHING;

-- Review chunks (5 rows total) to drive reviews_tsv and test RAG
-- Qamaria (2 chunks)
INSERT INTO review_chunks (
	google_maps_place_id, review_id, chunk_index, chunk_text, review_text, review_datetime_utc, chunk_tsv
) VALUES
('ChIJ6e1KBAAdVIgRBXdsuBE6sys','r-qam-1',0,
 'Booth-style seating with pillows makes it easy to study for hours. Garage parking is free behind the building.',
 'Booth-style seating with pillows makes it easy to study for hours. Garage parking is free behind the building.',
	TIMESTAMPTZ '2025-08-20 10:00:00+00', to_tsvector('english', 'Booth-style seating with pillows makes it easy to study for hours. Garage parking is free behind the building.')),
('ChIJ6e1KBAAdVIgRBXdsuBE6sys','r-qam-2',1,
 'They serve rich Yemeni coffee and wild cheesecake flavors. Outlets are solid along the wall.',
 'They serve rich Yemeni coffee and wild cheesecake flavors. Outlets are solid along the wall.',
	TIMESTAMPTZ '2025-08-18 15:30:00+00', to_tsvector('english', 'They serve rich Yemeni coffee and wild cheesecake flavors. Outlets are solid along the wall.'));

-- Amélie’s Park Road (2 chunks)
INSERT INTO review_chunks (
	google_maps_place_id, review_id, chunk_index, chunk_text, review_text, review_datetime_utc, chunk_tsv
) VALUES
('ChIJvVxqVlSeVogRz786_h7uyiQ','r-ame-1',0,
 'Large space with a row of booth-style seats and discreet outlet strips. Ideal for remote work.',
 'Large space with a row of booth-style seats and discreet outlet strips. Ideal for remote work.',
	TIMESTAMPTZ '2025-06-15 14:10:00+00', to_tsvector('english', 'Large space with a row of booth-style seats and discreet outlet strips. Ideal for remote work.')),
('ChIJvVxqVlSeVogRz786_h7uyiQ','r-ame-2',1,
 'Cinnamon rolls are excellent and you can even order a whole pot of tea—just ask!',
 'Cinnamon rolls are excellent and you can even order a whole pot of tea—just ask!',
	TIMESTAMPTZ '2025-06-12 09:05:00+00', to_tsvector('english', 'Cinnamon rolls are excellent and you can even order a whole pot of tea—just ask!'));

-- CHNO (1 chunk)
INSERT INTO review_chunks (
	google_maps_place_id, review_id, chunk_index, chunk_text, review_text, review_datetime_utc, chunk_tsv
) VALUES
('ChIJWy8-QoM7TG8RttGA2Fc3yok','r-chno-1',0,
 'Hidden-gem vibes with Honeybear Bake Shop pastries—watch for their cinnamon rolls. Splendid herbal tea lineup.',
 'Hidden-gem vibes with Honeybear Bake Shop pastries—watch for their cinnamon rolls. Splendid herbal tea lineup.',
	TIMESTAMPTZ '2025-08-10 11:00:00+00', to_tsvector('english', 'Hidden-gem vibes with Honeybear Bake Shop pastries—watch for their cinnamon rolls. Splendid herbal tea lineup.'));

-- Archive CLT (1 chunk)
INSERT INTO review_chunks (
	google_maps_place_id, review_id, chunk_index, chunk_text, review_text, review_datetime_utc, chunk_tsv
) VALUES
('ChIJzUFGL22hVogRXuG-SGJB45k','r-archive-1',0,
 'Warm, welcoming vibe with Black culture memorabilia. Great for reading or laptop work; free parking available.',
 'Warm, welcoming vibe with Black culture memorabilia. Great for reading or laptop work; free parking available.',
	TIMESTAMPTZ '2025-04-16 20:54:00+00', to_tsvector('english', 'Warm, welcoming vibe with Black culture memorabilia. Great for reading or laptop work; free parking available.'));

-- Sunflour (0 chunks) — relies on place-only FTS/metadata for testing

COMMIT;

