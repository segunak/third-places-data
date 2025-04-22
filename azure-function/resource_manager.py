"""
Resource Manager module that provides singleton instances of shared resources.

This module implements a thread-safe singleton pattern for resource management
across Azure Function executions, optimizing resource usage by reusing instances
when possible rather than creating new ones for every invocation.
"""

import os
import json
import logging
import dotenv
import threading
from typing import Dict, Any, Optional

from place_data_providers import PlaceDataProviderFactory
import airtable_client as ac

# Global configuration dictionary accessible module-wide
_config = {}

# Thread-local storage for resource instances
_thread_local = threading.local()

def get_config(key: str, default_value=None) -> Any:
    """
    Get a configuration value by key.
    
    Args:
        key (str): Configuration key to retrieve
        default_value: Default value to return if key doesn't exist
        
    Returns:
        Any: The configuration value, or default_value if not found
    """
    return _config.get(key, default_value)

def set_config(key: str, value: Any) -> None:
    """
    Set a configuration value by key.
    
    Args:
        key (str): Configuration key to set
        value (Any): Value to associate with the key
    """
    _config[key] = value
    
def from_request(req) -> None:
    """
    Initialize configuration from an HTTP request.
    
    This extracts common parameters from either query params or JSON body and 
    sets them in the module-level configuration.
    
    Args:
        req: HTTP request object with params and get_json() method
    """
    # Check if we need to load environment variables
    if 'FUNCTIONS_WORKER_RUNTIME' not in os.environ:
        logging.info('Loading environment variables from .env file')
        dotenv.load_dotenv()
    
    # Get query parameters
    provider_type = req.params.get('provider_type')
    force_refresh = req.params.get('force_refresh', '').lower() == 'true'
    sequential_mode = req.params.get('sequential_mode', '').lower() == 'true'
    city = req.params.get('city', 'charlotte')
    
    # If provider_type not in query params, try JSON body
    if not provider_type:
        try:
            req_body = req.get_json()
            provider_type = req_body.get('provider_type', provider_type)
            force_refresh = req_body.get('force_refresh', force_refresh)
            sequential_mode = req_body.get('sequential_mode', sequential_mode)
            city = req_body.get('city', city)
        except ValueError:
            # If JSON parsing fails, just use what we got from query params
            logging.info('No JSON body or invalid JSON in request')
    
    # Set config values from request
    if provider_type:
        set_config('provider_type', provider_type)
    else:
        logging.warning('No provider_type specified in request')
    
    set_config('force_refresh', force_refresh)
    set_config('sequential_mode', sequential_mode)
    set_config('city', city)
    
    logging.info(f"Configuration initialized from request: provider_type={provider_type}, "
                 f"force_refresh={force_refresh}, sequential_mode={sequential_mode}, city={city}")

def from_dict(data: Dict[str, Any]) -> None:
    """
    Initialize configuration from a dictionary.
    
    Args:
        data (Dict[str, Any]): Dictionary containing configuration values
    """
    # Update global config with values from data dictionary
    global _config
    if data:
        for key, value in data.items():
            _config[key] = value
        
        logging.info(f"Configuration initialized from dictionary: {data}")

def to_dict() -> Dict[str, Any]:
    """
    Convert current configuration to a dictionary.
    
    Returns:
        Dict[str, Any]: Dictionary containing all configuration values
    """
    return dict(_config)

def get_data_provider(provider_type: Optional[str] = None):
    """
    Get or create a data provider.
    
    This method uses thread-local storage to cache providers per thread, ensuring
    efficient reuse of provider instances.
    
    Args:
        provider_type (Optional[str]): Type of provider ('google', 'outscraper').
                                      If None, uses provider_type from config.
    
    Returns:
        PlaceDataProvider: A provider instance
        
    Raises:
        ValueError: If no provider_type is provided or available in config
    """
    if not provider_type:
        provider_type = get_config('provider_type')
        
    if not provider_type:
        raise ValueError("provider_type must be specified either directly or in config")
    
    # Check if we already have a provider of this type in thread-local storage
    if not hasattr(_thread_local, 'providers'):
        _thread_local.providers = {}
    
    if provider_type not in _thread_local.providers:
        # Create a new provider and store it in thread-local storage
        _thread_local.providers[provider_type] = PlaceDataProviderFactory.get_provider(provider_type)
        
    return _thread_local.providers[provider_type]

def get_airtable_client(provider_type: Optional[str] = None, sequential_mode: Optional[bool] = None):
    """
    Get or create an AirtableClient.
    
    This method uses thread-local storage to cache clients per thread, ensuring
    efficient reuse of client instances.
    
    Args:
        provider_type (Optional[str]): Type of provider for the AirtableClient.
                                      If None, uses provider_type from config.
        sequential_mode (Optional[bool]): Whether to use sequential mode.
                                        If None, uses sequential_mode from config.
    
    Returns:
        AirtableClient: An AirtableClient instance
        
    Raises:
        ValueError: If no provider_type is provided or available in config
    """
    if not provider_type:
        provider_type = get_config('provider_type')
        
    if not provider_type:
        raise ValueError("provider_type must be specified either directly or in config")
    
    if sequential_mode is None:
        sequential_mode = get_config('sequential_mode', False)
    
    # Check if we already have a client in thread-local storage
    if not hasattr(_thread_local, 'airtable_clients'):
        _thread_local.airtable_clients = {}
    
    # Create a unique key for each combination of provider_type and sequential_mode
    client_key = f"{provider_type}_{sequential_mode}"
    
    if client_key not in _thread_local.airtable_clients:
        # Create a new client and store it in thread-local storage
        _thread_local.airtable_clients[client_key] = ac.AirtableClient(provider_type, sequential_mode)
        
    return _thread_local.airtable_clients[client_key]

def reset():
    """
    Reset all resource manager state.
    Clears configuration and thread-local resources.
    """
    global _config
    _config = {}
    
    if hasattr(_thread_local, 'providers'):
        _thread_local.providers = {}
        
    if hasattr(_thread_local, 'airtable_clients'):
        _thread_local.airtable_clients = {}