
# Outscraper

Details about [Outscraper](https://outscraper.com/) and it's role in helping to process reviews.

## Random

- See [this page](https://outscraper.com/place-id-feature-id-cid/) for a `google_id` explainer. I have found no use for it but it's returned by the Outscraper API.

## Webhooks

Outscraper allows users to write a custom webhook and have them hit that endpoint with the response of requesting reviews. You can view the result of webhook calls [here](https://app.outscraper.com/webhook-calls).

Visit the [Integrations](https://app.outscraper.com/integrations) page to set the webhook URL. Read through the [Access Keys](https://learn.microsoft.com/en-us/azure/azure-functions/function-keys-how-to?tabs=azure-portal) page for Azure Functions to understand how authentication works. In summary, provide an endpoint that grants access to 1 function (use a Function Key), rather than all of them.

## Response Format

For testing Outscraper webhooks locally. The `results_location` expires after 24 hours or so. To get a new one, go to the Outscraper portal, make a Google Maps API reviews request, and then go to <https://app.outscraper.com/api-usage> to get the `results_location`.

```json
{
    "id": "your-request-id",
    "user_id": "your-user-id",
    "status": "SUCCESS",
    "api_task": true,
    "results_location": "https://api.app.outscraper.com/requests/YXV0aDB8NjNhMzRkZGRjNmRmNDM5MGJmM2ZkMzZjLDIwMjQwODE3MjA1OTM1eHM0YQ",
    "quota_usage": [
        {
            "product_name": "Google Maps Data",
            "quantity": 1
        }
    ]
}
```

When you're using Outscraper's Python API this is what the result of calling their `google_maps_reviews` returns. It's a list of dictionaries.

```python
{'query': 'ChIJJ1k-2i6gVogRYNxihxv5ONI', 'name': 'Amélie’s French Bakery & Café | Carmel Commons', 'name_for_emails': 'Amélie’S French Bakery And Café | Carmel Commons', 'place_id': 'ChIJJ1k-2i6gVogRYNxihxv5ONI', 'google_id': '0x8856a02eda3e5927:0xd238f91b8762dc60', 'full_address': '7715 Pineville-Matthews Rd Unit 34B, Charlotte, NC 28226', 'borough': 'McAlpine', 'street': '7715 Pineville-Matthews Rd Unit 34B', 'postal_code': '28226', 'area_service': False, 'country_code': 'US', 'country': 'United States of America', 'city': 'Charlotte', 'us_state': 'North Carolina', 'state': 'North Carolina', 'plus_code': '867X34PX+FP', 'latitude': 35.0861975, 'longitude': -80.85066499999999, 'h3': '8944d84ae27ffff', 'time_zone': 'America/New_York', 'popular_times': [{...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}], 'site': 'http://www.ameliesfrenchbakery.com/?utm_source=local', 'phone': '+1 704-376-1782', 'type': 'Bakery', 'logo': 'https://lh3.googleusercontent.com/-QD2bj7tNKB8/AAAAAAAAAAI/AAAAAAAAAAA/AQi-ylqiyPo/s44-p-k-no-ns-nd/photo.jpg', 'description': 'This charming gathering place draws a crowd for its pastries, light fare & coffee drinks.', 'typical_time_spent': 'People typically spend 15 min to 1 hr here', 'located_in': 'Carmel Commons', 'located_google_id': '0x88569d073037a283:0x407bb9a1c726e55e', 'category': 'Bakery', 'subtypes': 'Bakery, Coffee shop, French restaurant, Wedding bakery, Wholesale bakery, Wi-Fi spot', 'posts': None, 'reviews_tags': ['macaroons', 'soup', 'breakfast sandwich', 'espresso', 'patio', 'fruit tart', 'caramel brownie', 'chai latte', 'french press', 'chocolate mousse'], 'rating': 4.5, 'reviews': 1742, 'reviews_data': [{...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, {...}, ...], 'photos_count': 1152, 'cid': '15148131243291499616', 'reviews_link': 'https://search.google.com/local/reviews?placeid=ChIJJ1k-2i6gVogRYNxihxv5ONI&authuser=0&hl=en&gl=US', 'reviews_id': '-3298612830418052000', 'photo': 'https://lh5.googleusercontent.com/p/AF1QipP44c8672a5AUgingSIuB6JqYmxjWTLKFlSINAU=w800-h500-k-no', 'street_view': 'https://lh5.googleusercontent.com/p/AF1QipMUx6LEfIAyKTkgnusVXi4ZuxVfnybbDWx3vdFq=w1600-h1000-k-no', 'working_hours_old_format': 'Monday:7AM-7PM|Tuesday:7AM-7PM|Wednesday:7AM-7PM|Thursday:7AM-7PM|Friday:7AM-9PM|Saturday:7AM-9PM|Sunday:7AM-7PM', 'working_hours': {'Monday': '7AM-7PM', 'Tuesday': '7AM-7PM', 'Wednesday': '7AM-7PM', 'Thursday': '7AM-7PM', 'Friday': '7AM-9PM', 'Saturday': '7AM-9PM', 'Sunday': '7AM-7PM'}, 'other_hours': None, 'business_status': 'OPERATIONAL', 'about': {'From the business': {...}, 'Service options': {...}, 'Accessibility': {...}, 'Dining options': {...}, 'Amenities': {...}, 'Crowd': {...}, 'Planning': {...}, 'Payments': {...}, 'Parking': {...}, 'Other': {...}}, 'range': '$$', 'reviews_per_score': None, 'reviews_per_score_1': None, 'reviews_per_score_2': None, 'reviews_per_score_3': None, 'reviews_per_score_4': None, 'reviews_per_score_5': None, 'reservation_links': None, 'booking_appointment_link': 'https://food.google.com/chooseprovider?restaurantId=/g/1tfv1816&g2lbs=AOHF13k0cN3OzkD_Wp9mxwHsk-4uEIjeX69ELpcDlbV6msv7CglZV45DtxbrWTpuht05RGcAv_rIlrt6MVlyP6xzcSno5Zk-_7p72LRoqGhSh9-YGLBVyO4%3D&hl=en-US&gl=us&fo_m=MfohQo559jFvMUOzJVpjPL1YMfZ3bInYwBDuMfaXTPp5KXh-&utm_source=tactile&gei=VFjKZqTVCrKF0PEP3OngqQk&ei=VFjKZqTVCrKF0PEP3OngqQk&fo_s=OA,SOE&opi=79508299&orderType=2&foub=mcpp', 'menu_link': 'https://www.toasttab.com/amelies-french-bakery-carmel-commons-7715-pineville-matthews-road-space-34b/v3#!/', 'order_links': None, 'owner_id': '103411538791022838693', ...}
```
