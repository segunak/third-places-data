# Retrieval-Augmented Generation

## Configuration

- **Foundry Endpoint**: `https://foundry-third-places.services.ai.azure.com/`
- **Embedding Model**: `text-embedding-3-small`
- **Embedding Dimensions**: 1536
- **Cosmos DB Database**: `third-places`
- **Cosmos DB Account**: `cosmos-third-places`

## Embedding Text Composition

### Places Embedding

Combines semantic fields for place-level search:

```
{place} | {description} | {neighborhood} | {address} | {type} | {tags joined} | {category} | {subtypes joined} | {about flattened}
```

Example:
```
Waterbean Coffee | Here at Waterbean Coffee, we always try to stay original... | Davidson | 428 S Main St A, Davidson, NC 28036 | Café | Coffee, Study Spot, WiFi | coffee shops | Coffee shop | Dine-in: yes, Coffee: yes, Casual: yes, Cosy: yes
```

### Chunks Embedding

Combines review text with place context for review-level search:

```
{reviewText} | {placeName} | {neighborhood} | {placeType} | {placeTags joined} | {ownerAnswer if present}
```

Example:
```
Went here visiting my sister and stopped in. Got the snickers doodle latte and its so delicious! Cute place too! | Waterbean Coffee | Davidson | Café | Coffee, Study Spot, WiFi
```

## Places Container Field Mapping

| Cosmos DB Field | Source | Source Path | Source Field Name |
|-----------------|--------|-------------|-------------------|
| **SYSTEM FIELDS** |
| `id` | Airtable | — | `Google Maps Place Id` |
| `embedding` | Generated | — | (computed) |
| `embeddingText` | Generated | — | (computed) |
| `lastSynced` | Generated | — | (computed) |
| **AIRTABLE FIELDS** |
| `address` | Airtable | — | `Address` |
| `appleMapsProfileUrl` | Airtable | — | `Apple Maps Profile URL` |
| `comments` | Airtable | — | `Comments` |
| `createdTime` | Airtable | — | `Created Time` |
| `curatorPhotos` | Airtable | — | `Curator Photos` |
| `description` | Airtable | — | `Description` |
| `facebook` | Airtable | — | `Facebook` |
| `featured` | Airtable | — | `Featured` |
| `freeWifi` | Airtable | — | `Free Wi-Fi` |
| `googleMapsPlaceId` | Airtable | — | `Google Maps Place Id` |
| `googleMapsProfileUrl` | Airtable | — | `Google Maps Profile URL` |
| `hasCinnamonRolls` | Airtable | — | `Has Cinnamon Rolls` |
| `hasDataFile` | Airtable | — | `Has Data File` |
| `instagram` | Airtable | — | `Instagram` |
| `lastModifiedTime` | Airtable | — | `Last Modified Time` |
| `lastRevalidated` | Airtable | — | `Last Revalidated` |
| `latitude` | Airtable | — | `Latitude` |
| `linkedIn` | Airtable | — | `LinkedIn` |
| `longitude` | Airtable | — | `Longitude` |
| `neighborhood` | Airtable | — | `Neighborhood` |
| `operational` | Airtable | — | `Operational` |
| `parking` | Airtable | — | `Parking` |
| `photos` | Airtable | — | `Photos` |
| `place` | Airtable | — | `Place` |
| `purchaseRequired` | Airtable | — | `Purchase Required` |
| `size` | Airtable | — | `Size` |
| `tags` | Airtable | — | `Tags` |
| `tikTok` | Airtable | — | `TikTok` |
| `twitter` | Airtable | — | `Twitter` |
| `type` | Airtable | — | `Type` |
| `website` | Airtable | — | `Website` |
| `youTube` | Airtable | — | `YouTube` |
| **JSON: details.raw_data FIELDS** |
| `popularTimes` | JSON | `details.raw_data` | `popular_times` |
| `typicalTimeSpent` | JSON | `details.raw_data` | `typical_time_spent` |
| `workingHours` | JSON | `details.raw_data` | `working_hours` |
| `about` | JSON | `details.raw_data` | `about` |
| `category` | JSON | `details.raw_data` | `category` |
| `subtypes` | JSON | `details.raw_data` | `subtypes` |
| `reviewsLink` | JSON | `details.raw_data` | `reviews_link` |
| `streetView` | JSON | `details.raw_data` | `street_view` |
| `phone` | JSON | `details.raw_data` | `phone` |
| `locatedIn` | JSON | `details.raw_data` | `located_in` |
| `reviewsTags` | JSON | `details.raw_data` | `reviews_tags` |
| `placeRating` | JSON | `details.raw_data` | `rating` |
| `reviewsCount` | JSON | `details.raw_data` | `reviews` |
| **JSON: photos.raw_data FIELDS** |
| `photosData` | JSON | `photos.raw_data` | `photos_data` |

---

## Chunks Container Field Mapping

| Cosmos DB Field | Source | Source Path | Source Field Name |
|-----------------|--------|-------------|-------------------|
| **SYSTEM FIELDS** |
| `id` | JSON | `reviews.raw_data.reviews_data[]` | `review_id` |
| `placeId` | JSON | `reviews.raw_data` | `place_id` |
| `embedding` | Generated | — | (computed) |
| `embeddingText` | Generated | — | (computed) |
| `lastSynced` | Generated | — | (computed) |
| **DENORMALIZED PLACE FIELDS** |
| `placeName` | Airtable | — | `Place` |
| `neighborhood` | Airtable | — | `Neighborhood` |
| `address` | Airtable | — | `Address` |
| `googleMapsProfileUrl` | Airtable | — | `Google Maps Profile URL` |
| `appleMapsProfileUrl` | Airtable | — | `Apple Maps Profile URL` |
| `placeType` | Airtable | — | `Type` |
| `placeTags` | Airtable | — | `Tags` |
| **REVIEW FIELDS** |
| `reviewText` | JSON | `reviews.raw_data.reviews_data[]` | `review_text` |
| `reviewLink` | JSON | `reviews.raw_data.reviews_data[]` | `review_link` |
| `reviewRating` | JSON | `reviews.raw_data.reviews_data[]` | `review_rating` |
| `reviewDatetimeUtc` | JSON | `reviews.raw_data.reviews_data[]` | `review_datetime_utc` |
| `reviewTimestamp` | JSON | `reviews.raw_data.reviews_data[]` | `review_timestamp` |
| `reviewQuestions` | JSON | `reviews.raw_data.reviews_data[]` | `review_questions` |
| **OWNER RESPONSE** |
| `hasOwnerResponse` | JSON | `reviews.raw_data.reviews_data[]` | (computed from `owner_answer != null`) |
| `ownerAnswer` | JSON | `reviews.raw_data.reviews_data[]` | `owner_answer` |
| **REVIEW MEDIA** |
| `reviewImgUrls` | JSON | `reviews.raw_data.reviews_data[]` | `review_img_urls` |
| **AGGREGATE CONTEXT** |
| `reviewsTags` | JSON | `details.raw_data` | `reviews_tags` |
| `placeRating` | JSON | `details.raw_data` | `rating` |
| `placeReviewsCount` | JSON | `details.raw_data` | `reviews` |