# Third Places Data Caching System

## Overview

The Third Places Data project implements a robust caching mechanism to optimize data retrieval operations, reduce API costs, and improve performance. This document explains how the caching system works, its configuration options, and best practices for working with cached data.

## Why Caching?

The application heavily relies on third-party data providers like Outscraper and Google Maps to fetch place details, reviews, and photos. These services:

1. Have usage quotas and rate limits
2. Charge per API call
3. May have varying response times

By implementing a cache-first approach, we:

- **Reduce costs** by minimizing redundant API calls
- **Improve performance** by serving cached data when appropriate
- **Ensure reliability** by reducing dependency on external services
- **Provide offline capabilities** by maintaining a local copy of previously fetched data

## Cache Storage Implementation

The system uses GitHub repository storage as the primary cache medium:

- Data is stored as JSON files in the `data/places/{city_name}/{place_id}.json` directory structure
- Each cache file includes metadata about when it was last updated
- Files are retrieved using the GitHub API through the `fetch_data_github()` function
- New/updated data is saved using the `save_data_github()` function

## Cache Refresh Strategy

The system implements a time-based cache invalidation strategy with the following configurable parameters:

| Data Type | Default Refresh Interval | Environment Variable | Description |
|-----------|---------------------------|----------------------|-------------|
| All data types | 30 days | `DEFAULT_CACHE_REFRESH_INTERVAL` | General fallback for any data type |

## Configuration Options

The caching behavior can be customized through environment variables:

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DEFAULT_CACHE_REFRESH_INTERVAL` | 30 | Default number of days before cache refresh |
| `FORCE_REFRESH_DATA` | false | When set to "true", bypass cache and always fetch fresh data |
| `DEFAULT_PLACE_DATA_PROVIDER` | outscraper | The data provider to use (outscraper or google_maps) |

These variables are set in the `local.settings.json` file for local development and in the Azure Function App settings for production.

## Cache Flow Process

The caching system follows this decision tree:

1. Check if `FORCE_REFRESH_DATA` is enabled
   - If true, skip cache check and fetch fresh data
   - If false, proceed to cache check

2. Check if cached data exists for the place
   - If no cache exists, fetch fresh data
   - If cache exists, check its age

3. Evaluate cache freshness
   - Compare the `last_updated` timestamp against the refresh interval
   - If cache is fresh (within interval), use cached data
   - If cache is stale (older than interval), fetch fresh data

4. Update cache
   - After fetching fresh data, save it to the cache
   - Update the `last_updated` timestamp
   - Update the Airtable record's `Has Data File` and `Last Updated` fields

## Cache Validation

The `is_cache_valid()` function in `helper_functions.py` determines whether cached data is still fresh:

```python
def is_cache_valid(cached_data: Dict, refresh_interval_days: int) -> bool:
    """
    Checks if the cached data is still valid based on the last updated timestamp.
    
    Args:
        cached_data (Dict): The cached data containing a 'last_updated' timestamp
        refresh_interval_days (int): Number of days after which the cache should be refreshed
    
    Returns:
        bool: True if the cache is still valid, False if it's stale and needs to be refreshed
    """
    try:
        last_updated_str = cached_data.get('last_updated')
        if not last_updated_str:
            return False
            
        last_updated = datetime.fromisoformat(last_updated_str)
        refresh_threshold = datetime.now() - timedelta(days=refresh_interval_days)
        
        return last_updated > refresh_threshold
    except Exception as e:
        logging.error(f"Error checking cache validity: {e}")
        return False
```

## Best Practices

1. **Do not manually edit cache files** - Let the system manage cache creation and updates.

2. **Use appropriate refresh intervals** - Adjust the refresh intervals based on data volatility.

3. **Clear cache when needed** - If specific data needs immediate refresh, you can:
   - Temporarily set `FORCE_REFRESH_DATA` to `true`
   - Delete specific cache files to force refresh for those places
   
4. **Monitor cache usage** - Cache hits/misses are logged at INFO level.

## Testing with the Cache System

When running tests:

1. The test environment will use its own version of the cache settings
2. Tests are designed to mock the cache functions to prevent actual API calls
3. The `test_function_app.py` includes tests for cache-related endpoints

## Troubleshooting

Common issues:

- **Stale data**: If you need fresh data immediately, set `FORCE_REFRESH_DATA=true` in your environment variables.
  
- **Missing cache files**: If cache files are missing, check GitHub repository permissions and ensure the `GITHUB_PERSONAL_ACCESS_TOKEN` is valid.

- **Inconsistent cache behavior**: Check your environment variables and ensure they're properly configured in all environments.

- **Performance issues**: If retrieving cached data is slow, consider implementing local file system caching as a fallback.