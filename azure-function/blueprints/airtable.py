import json
import logging
import re
import azure.functions as func
import azure.durable_functions as df
from services.airtable_service import AirtableService
from services.place_data_service import PlaceDataProviderFactory

bp = df.Blueprint()


# ======================================================
# Airtable Enrichment Functions
# ======================================================


@bp.function_name(name="EnrichAirtableBase")
@bp.route(route="enrich-airtable-base")
@bp.durable_client_input(client_name="client")
async def enrich_airtable_base(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request for Airtable base enrichment.")

    try:
        provider_type = req.params.get('provider_type')
        force_refresh = req.params.get('force_refresh', '').lower() == 'true'
        sequential_mode = req.params.get('sequential_mode', '').lower() == 'true'
        city = req.params.get('city')
        # view parameter specifies which Airtable view to use. Defaults to "Production".
        # Pass "Insufficient" to only process records needing enrichment.
        view = req.params.get('view', 'Production')

        if not provider_type:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: provider_type",
                    "data": None,
                    "error": "The provider_type parameter is required"
                }),
                status_code=400,
                mimetype="application/json"
            )
        if not city:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: city",
                    "data": None,
                    "error": "The city parameter is required"
                }),
                status_code=400,
                mimetype="application/json"
            )

        logging.info(f"Starting enrichment with parameters: city={city}, force_refresh={force_refresh}, "
                     f"sequential_mode={sequential_mode}, provider_type={provider_type}, "
                     f"view={view}")

        config_dict = {
            "force_refresh": force_refresh,
            "sequential_mode": sequential_mode,
            "provider_type": provider_type,
            "city": city,
            "view": view
        }

        instance_id = await client.start_new("enrich_airtable_base_orchestrator", client_input=config_dict)
        logging.info(f"Started orchestration with ID: {instance_id}")

        response = client.create_check_status_response(req, instance_id)
        return response

    except Exception as ex:
        logging.error(f"Error encountered while starting the enrichment orchestration: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting the enrichment orchestration.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500
        )


@bp.orchestration_trigger(context_name="context")
def enrich_airtable_base_orchestrator(context: df.DurableOrchestrationContext):
    try:
        logging.info("enrich_airtable_base_orchestrator started.")
        config_dict = context.get_input() or {}
        force_refresh = config_dict.get("force_refresh", False)
        sequential_mode = config_dict.get("sequential_mode", False)
        provider_type = config_dict.get("provider_type", None)
        city = config_dict.get("city")
        view = config_dict.get("view", "Production")

        if not city:
            raise ValueError("Missing required parameter: city")

        all_third_places = yield context.call_activity(
            'get_all_third_places',
            {"config": config_dict}
        )

        if view != "Production" and not all_third_places:
            logging.info(f"No records were found in the '{view}' view to enrich.")
            result = {
                "success": True,
                "message": f"No records were found in the '{view}' view to enrich.",
                "data": {
                    "total_places_processed": 0,
                    "total_places_enriched": 0,
                    "places_enriched": []
                },
                "error": None
            }
            return result

        results = []
        from constants import MAX_THREAD_WORKERS
        concurrency_limit = MAX_THREAD_WORKERS

        if sequential_mode:
            logging.info(f"Running enrichment in SEQUENTIAL mode for {len(all_third_places)} places")
            for place in all_third_places:
                activity_input = {
                    "place": place,
                    "provider_type": provider_type,
                    "city": city,
                    "force_refresh": force_refresh,
                    "sequential_mode": sequential_mode,
                    "view": view
                }
                result = yield context.call_activity("enrich_single_place", activity_input)
                results.append(result)
        else:
            logging.info(f"Running enrichment in PARALLEL mode with concurrency={concurrency_limit} for {len(all_third_places)} places")
            for i in range(0, len(all_third_places), concurrency_limit):
                batch = all_third_places[i:i+concurrency_limit]
                batch_tasks = []
                for place in batch:
                    activity_input = {
                        "place": place,
                        "provider_type": provider_type,
                        "city": city,
                        "force_refresh": force_refresh,
                        "sequential_mode": sequential_mode,
                        "view": view
                    }
                    batch_tasks.append(context.call_activity("enrich_single_place", activity_input))
                batch_results = yield context.task_all(batch_tasks)
                results.extend(batch_results)

        # Categorize results into enriched, not found, skipped, and failed
        actually_updated_places = []
        not_found_places = []
        skipped_places = []
        failed_places = []
        
        for place in results:
            if not place:
                continue
                
            # Check if place was enriched (has updates with updated=True)
            if place.get('field_updates') and any(updates.get("updated") for updates in place.get('field_updates', {}).values()):
                actually_updated_places.append(place)
            # Check if place was not found (sentinel case)
            elif place.get('status') == 'failed' and 'NO_PLACE_FOUND' in place.get('message', ''):
                not_found_places.append({
                    'place_name': place.get('place_name'),
                    'place_id': place.get('place_id'),
                    'record_id': place.get('record_id'),
                    'message': place.get('message', '')
                })
            # Skipped places (intentionally skipped, not a failure)
            elif place.get('status') == 'skipped':
                skipped_places.append({
                    'place_name': place.get('place_name'),
                    'place_id': place.get('place_id'),
                    'record_id': place.get('record_id'),
                    'message': place.get('message', 'Place skipped')
                })
            # Other failures (actual errors)
            elif place.get('status') == 'failed':
                failed_places.append({
                    'place_name': place.get('place_name'),
                    'place_id': place.get('place_id'),
                    'record_id': place.get('record_id'),
                    'message': place.get('message', 'Unknown error')
                })
        
        total_places_enriched = len(actually_updated_places)
        total_places_not_found = len(not_found_places)
        total_places_skipped = len(skipped_places)
        total_places_failed = len(failed_places)
        
        message = "Airtable base enrichment processed successfully."
        if view != "Production":
            message = f"Airtable base enrichment processed {len(results)} records from '{view}' view. {total_places_enriched} enriched, {total_places_not_found} not found, {total_places_skipped} skipped, {total_places_failed} failed."
        else:
            message = f"Airtable base enrichment processed {len(results)} records. {total_places_enriched} enriched, {total_places_not_found} not found, {total_places_skipped} skipped, {total_places_failed} failed."
        
        result = {
            "success": True,
            "message": message,
            "data": {
                "total_places_processed": len(results),
                "total_places_enriched": total_places_enriched,
                "total_places_not_found": total_places_not_found,
                "total_places_skipped": total_places_skipped,
                "total_places_failed": total_places_failed,
                "places_enriched": actually_updated_places,
                "places_not_found": not_found_places,
                "places_skipped": skipped_places,
                "places_failed": failed_places
            },
            "error": None
        }
        return result
    except Exception as ex:
        logging.error(f"Critical error in enrich_airtable_base_orchestrator: {ex}", exc_info=True)
        error_response = {
            "success": False,
            "message": "Error occurred during the enrichment orchestration.",
            "data": None,
            "error": str(ex)
        }
        return error_response


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("enrich_single_place")
def enrich_single_place(activityInput):
    try:
        place = activityInput.get("place")
        provider_type = activityInput.get("provider_type")
        city = activityInput.get("city")
        force_refresh = activityInput.get("force_refresh", False)
        sequential_mode = activityInput.get("sequential_mode", False)
        view = activityInput.get("view", "Production")
        place_name = place['fields']['Place'] if place and 'fields' in place and 'Place' in place['fields'] else "Unknown Place"

        if not provider_type or not city:
            return {
                "place_name": place_name,
                "status": "failed",
                "message": "Missing required parameter: provider_type or city",
                "field_updates": {}
            }

        airtable_client = AirtableService(provider_type, sequential_mode, view)
        result = airtable_client.enrich_single_place(place, provider_type, city, force_refresh)
        return result
    except Exception as ex:
        logging.error(f"Error enriching place {place_name}: {ex}", exc_info=True)
        return {
            "place_name": place_name,
            "status": "failed",
            "message": f"Error processing enrichment: {str(ex)}",
            "field_updates": {}
        }


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("get_all_third_places")
def get_all_third_places(activityInput):
    try:
        config_dict = activityInput.get("config", {})
        
        city = config_dict.get('city')
        provider_type = config_dict.get('provider_type')
        sequential_mode = config_dict.get('sequential_mode', False)
        view = config_dict.get('view', 'Production')

        if not city:
            logging.error("Cannot get Airtable Service - city is not set")
            return []

        if not provider_type:
            logging.error("Cannot get Airtable Service - provider_type is not set")
            return []

        airtable_client = AirtableService(provider_type, sequential_mode, view)
        return airtable_client.all_third_places

    except Exception as ex:
        logging.error(f"Error in get_all_third_places: {ex}", exc_info=True)
        # Re-raise so the orchestrator can surface a failure instead of silently proceeding.
        raise


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("refresh_single_place_operational_status")
def refresh_single_place_operational_status(activityInput):
    try:
        place = activityInput.get("place")
        provider_type = activityInput.get("provider_type")
        city = activityInput.get("city")
        place_name = place['fields']['Place'] if place and 'fields' in place and 'Place' in place['fields'] else "Unknown Place"
        if not provider_type or not city:
            return {
                "place_name": place_name,
                "update_status": "failed",
                "message": "Missing required parameter: provider_type or city"
            }

        airtable_client = AirtableService(provider_type)
        data_provider = PlaceDataProviderFactory.get_provider(provider_type)
        result = airtable_client.refresh_single_place_operational_status(place, data_provider)
        return result
    except Exception as ex:
        logging.error(f"Error refreshing operational status for {place_name}: {ex}", exc_info=True)
        return {
            "place_name": place_name,
            "update_status": "failed",
            "message": f"Error processing operational status refresh: {str(ex)}"
        }


@bp.function_name(name="RefreshAirtableOperationalStatuses")
@bp.route(route="refresh-airtable-operational-statuses")
@bp.durable_client_input(client_name="client")
async def refresh_airtable_operational_statuses(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request to refresh Airtable operational statuses.")

    try:
        provider_type = req.params.get('provider_type')
        sequential_mode = req.params.get('sequential_mode', '').lower() == 'true'
        city = req.params.get('city')

        if not provider_type:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: provider_type",
                    "data": None,
                    "error": "The provider_type parameter is required"
                }),
                status_code=400,
                mimetype="application/json"
            )
        if not city:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: city",
                    "data": None,
                    "error": "The city parameter is required"
                }),
                status_code=400,
                mimetype="application/json"
            )

        config_dict = {
            "provider_type": provider_type,
            "sequential_mode": sequential_mode,
            "city": city
        }
        instance_id = await client.start_new("refresh_airtable_operational_statuses_orchestrator", client_input=config_dict)
        logging.info(f"Started orchestration with ID: {instance_id}")
        response = client.create_check_status_response(req, instance_id)
        return response
    except Exception as ex:
        logging.error(f"Error encountered while starting the operational status refresh orchestration: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting the operational status refresh orchestration.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )


@bp.orchestration_trigger(context_name="context")
def refresh_airtable_operational_statuses_orchestrator(context: df.DurableOrchestrationContext):
    try:
        logging.info("refresh_airtable_operational_statuses_orchestrator started.")
        config_dict = context.get_input() or {}
        sequential_mode = config_dict.get("sequential_mode", False)
        provider_type = config_dict.get("provider_type", None)
        city = config_dict.get("city")

        if not city:
            raise ValueError("Missing required parameter: city")
        if not provider_type:
            raise ValueError("Missing required parameter: provider_type")
        all_third_places = yield context.call_activity(
            'get_all_third_places',
            {"config": config_dict}
        )

        results = []

        from constants import MAX_THREAD_WORKERS
        concurrency_limit = MAX_THREAD_WORKERS

        if sequential_mode:
            logging.info(f"Running operational status refresh in SEQUENTIAL mode for {len(all_third_places)} places")
            for place in all_third_places:
                activity_input = {
                    "place": place,
                    "provider_type": provider_type,
                    "city": city
                }
                result = yield context.call_activity("refresh_single_place_operational_status", activity_input)
                results.append(result)
        else:
            logging.info(f"Running operational status refresh in PARALLEL mode with concurrency={concurrency_limit} for {len(all_third_places)} places")

            for i in range(0, len(all_third_places), concurrency_limit):
                batch = all_third_places[i:i+concurrency_limit]
                batch_tasks = []
                for place in batch:
                    activity_input = {
                        "place": place,
                        "provider_type": provider_type,
                        "city": city
                    }
                    batch_tasks.append(context.call_activity("refresh_single_place_operational_status", activity_input))

                batch_results = yield context.task_all(batch_tasks)
                total_batches = (len(all_third_places) + concurrency_limit - 1) // concurrency_limit
                current_batch = i // concurrency_limit + 1
                logging.info(f"Processed batch {current_batch} of {total_batches} with {len(batch)} places")

                results.extend(batch_results)

        failed_updates = [res for res in results if res.get('update_status') == 'failed']
        result = {
            "success": len(failed_updates) == 0,
            "message": "Operational statuses refreshed successfully." if not failed_updates else "One or more operational status updates failed.",
            "data": results,
            "error": None if not failed_updates else f"{len(failed_updates)} failed updates"
        }
        return result
    except Exception as ex:
        logging.error(f"Critical error in refresh_airtable_operational_statuses_orchestrator: {ex}", exc_info=True)
        error_response = {
            "success": False,
            "message": "Error occurred during the operational status refresh orchestration.",
            "data": None,
            "error": str(ex)
        }
        return error_response


# ======================================================
# Airtable Health Check / Data Quality Functions
# ======================================================


def _validate_place_id_format(place_id: str) -> tuple[bool, str]:
    """
    Validate that a string looks like a valid Google Maps Place ID.
    
    Based on Google's documentation, Place IDs:
    - Are alphanumeric with underscores and hyphens
    - Have no spaces
    - Typically start with ChIJ, GhIJ, or other prefixes
    - Are usually 20+ characters but can be longer (no max length)
    
    Args:
        place_id: The Place ID string to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not place_id:
        return False, "Empty Place ID"
    
    if ' ' in place_id:
        return False, "Contains spaces"
    
    # Place IDs should only contain alphanumeric, underscore, hyphen, and sometimes equals
    # Based on examples: ChIJgUbEo8cfqokR5lP9_Wh_DaM, EicxMyBNYXJr...
    if not re.match(r'^[A-Za-z0-9_\-=]+$', place_id):
        return False, "Contains invalid characters (only alphanumeric, underscore, hyphen, equals allowed)"
    
    # Most valid Place IDs are at least 20 characters
    if len(place_id) < 20:
        return False, f"Too short ({len(place_id)} chars, expected 20+)"
    
    # Common prefixes for Google Place IDs
    valid_prefixes = ('ChIJ', 'GhIJ', 'EicxM', 'Eiox', 'EpID', 'IhoS')
    if not place_id.startswith(valid_prefixes):
        # Not an error, just a warning - some valid IDs have other formats
        return True, f"Unusual prefix (expected ChIJ/GhIJ/E.../I..., got {place_id[:4]})"
    
    return True, ""


def _check_required_fields(record: dict, required_fields: list[str]) -> list[dict]:
    """
    Check if a record has all required fields populated.
    
    Args:
        record: Airtable record with 'fields' dict
        required_fields: List of field names that must be non-empty
        
    Returns:
        List of issues found (empty if all fields present)
    """
    issues = []
    fields = record.get("fields", {})
    record_id = record.get("id", "unknown")
    place_name = fields.get("Place", "Unknown Place")
    
    for field_name in required_fields:
        value = fields.get(field_name)
        if not value or (isinstance(value, str) and not value.strip()):
            issues.append({
                "recordId": record_id,
                "placeName": place_name,
                "field": field_name,
                "issue": "Missing or empty"
            })
    
    return issues


@bp.function_name(name="AirtableHealthCheck")
@bp.route(route="airtable/health", methods=["GET"])
def airtable_health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Perform data quality checks on Airtable records.
    
    Checks performed:
    1. Duplicate Google Maps Place IDs
    2. Invalid Place ID format (spaces, invalid characters, too short)
    3. Missing required fields (Place, Address, Type, Neighborhood, 
       Google Maps Place Id, Google Maps Profile URL, Apple Maps Profile URL)
    
    Returns a detailed report of all data quality issues found.
    """
    logging.info("Airtable health check requested")
    
    # Required fields that must be non-empty
    required_fields = [
        "Place",
        "Address", 
        "Type",
        "Neighborhood",
        "Google Maps Place Id",
        "Google Maps Profile URL",
        "Apple Maps Profile URL"
    ]
    
    report = {
        "status": "healthy",
        "summary": {
            "totalRecords": 0,
            "duplicatePlaceIds": {"count": 0, "description": "Records with duplicate Google Maps Place IDs"},
            "invalidPlaceIds": {"count": 0, "description": "Records with invalid Place ID format"},
            "missingFields": {"count": 0, "description": "Records missing required fields"},
            "totalIssues": {"count": 0, "description": "Total number of data quality issues"}
        },
        "issues": {
            "duplicatePlaceIds": [],
            "invalidPlaceIds": [],
            "missingFields": []
        },
        "requiredFields": required_fields
    }
    
    try:
        # Get Airtable records
        # Note: provider_type is required by AirtableService but not used for read-only operations
        view = req.params.get('view', 'Production')
        airtable_service = AirtableService(provider_type="google", view=view)
        records = airtable_service.all_third_places
        
        if not records:
            report["summary"]["totalRecords"] = 0
            return func.HttpResponse(
                json.dumps(report, indent=2),
                status_code=200,
                mimetype="application/json"
            )
        
        report["summary"]["totalRecords"] = len(records)
        
        # Track Place IDs for duplicate detection
        place_id_occurrences = {}  # place_id -> list of records
        
        for record in records:
            fields = record.get("fields", {})
            record_id = record.get("id", "unknown")
            place_name = fields.get("Place", "Unknown Place")
            place_id = fields.get("Google Maps Place Id", "")
            
            # 1. Check for duplicate Place IDs
            if place_id:
                if place_id not in place_id_occurrences:
                    place_id_occurrences[place_id] = []
                place_id_occurrences[place_id].append({
                    "recordId": record_id,
                    "placeName": place_name
                })
            
            # 2. Validate Place ID format
            if place_id:
                is_valid, error_msg = _validate_place_id_format(place_id)
                if not is_valid or error_msg:
                    report["issues"]["invalidPlaceIds"].append({
                        "recordId": record_id,
                        "placeName": place_name,
                        "placeId": place_id,
                        "issue": error_msg,
                        "isError": not is_valid  # False = warning, True = error
                    })
            
            # 3. Check required fields
            missing_field_issues = _check_required_fields(record, required_fields)
            report["issues"]["missingFields"].extend(missing_field_issues)
        
        # Process duplicate Place IDs
        for place_id, occurrences in place_id_occurrences.items():
            if len(occurrences) > 1:
                report["issues"]["duplicatePlaceIds"].append({
                    "placeId": place_id,
                    "occurrences": occurrences,
                    "count": len(occurrences)
                })
        
        # Update summary counts
        report["summary"]["duplicatePlaceIds"]["count"] = len(report["issues"]["duplicatePlaceIds"])
        report["summary"]["invalidPlaceIds"]["count"] = len([
            i for i in report["issues"]["invalidPlaceIds"] if i.get("isError", True)
        ])
        report["summary"]["missingFields"]["count"] = len(report["issues"]["missingFields"])
        
        # Calculate total issues (only count actual errors, not warnings)
        total_issues = (
            report["summary"]["duplicatePlaceIds"]["count"] +
            report["summary"]["invalidPlaceIds"]["count"] +
            report["summary"]["missingFields"]["count"]
        )
        report["summary"]["totalIssues"]["count"] = total_issues
        
        # Determine overall status
        if total_issues > 0:
            report["status"] = "unhealthy"
        else:
            # Check for warnings (like unusual prefixes)
            warnings = [i for i in report["issues"]["invalidPlaceIds"] if not i.get("isError", True)]
            if warnings:
                report["status"] = "healthy_with_warnings"
        
        logging.info(f"Airtable health check complete: {total_issues} issues found")
        
        return func.HttpResponse(
            json.dumps(report, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error during Airtable health check: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "error": str(e),
                "message": "Failed to complete Airtable health check"
            }, indent=2),
            status_code=500,
            mimetype="application/json"
        )
