import json
import logging
import datetime
import azure.functions as func
import azure.durable_functions as df
from azure.durable_functions.models.DurableOrchestrationStatus import OrchestrationRuntimeStatus

# Define a blueprint for admin endpoints and register it in function_app.py
bp = df.Blueprint()


@bp.function_name(name="PurgeOrchestrations")
@bp.route(route="purge-orchestrations")
@bp.durable_client_input(client_name="client")
async def purge_orchestrations(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request to purge orchestration instances.")

    try:
        runtime_statuses = [
            OrchestrationRuntimeStatus.Failed,
            OrchestrationRuntimeStatus.Completed,
            OrchestrationRuntimeStatus.Terminated,
        ]

        purge_result = await client.purge_instance_history_by(
            created_time_from=datetime.datetime(1900, 1, 1),
            created_time_to=datetime.datetime.now(datetime.timezone.utc),
            runtime_status=runtime_statuses,
        )

        logging.info(
            f"Successfully purged orchestration instances. Instances deleted: {purge_result.instances_deleted}"
        )
        return func.HttpResponse(
            json.dumps(
                {
                    "message": "Purged orchestration instances.",
                    "instancesDeleted": purge_result.instances_deleted,
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as ex:
        logging.error(
            f"Error occurred while purging orchestrations: {str(ex)}", exc_info=True
        )
        if hasattr(ex, "response") and ex.response is not None:
            logging.error(f"HTTP Status Code: {ex.response.status_code}")
            logging.error(f"Response Content: {ex.response.content.decode()}")
        return func.HttpResponse(
            json.dumps(
                {"message": "Failed to purge orchestration instances.", "error": str(ex)}
            ),
            status_code=500,
            mimetype="application/json",
        )


@bp.function_name(name="SmokeTest")
@bp.route(route="smoke-test")
def smoke_test(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Received smoke test request.")
    try:
        req_body = req.get_json()
        logging.info(f"Request body: {req_body}")

        expected_key = "House"
        expected_value = "Martell"

        if req_body.get(expected_key, None) == expected_value:
            logging.info("Request body contains the correct allegiance.")
            return func.HttpResponse(
                json.dumps(
                    {
                        "message": "The Azure Function is operational and recognizes Dorne. Unbowed. Unbent. Unbroken.",
                    }
                ),
                status_code=200,
                mimetype="application/json",
            )
        else:
            logging.info(
                f"Incorrect allegiance provided. Expected {expected_value}, but got {req_body.get(expected_key, None)}"
            )
            return func.HttpResponse(
                json.dumps({"message": "Unexpected or incorrect allegiance provided."}),
                status_code=400,
                mimetype="application/json",
            )

    except Exception as ex:
        logging.error(f"Failed to parse request body as JSON. {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps(
                {
                    "message": "Invalid or missing JSON body. Are you sure you should be hitting this endpoint?",
                }
            ),
            status_code=400,
            mimetype="application/json",
        )
