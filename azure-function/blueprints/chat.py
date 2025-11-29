"""
Chat blueprint for AI-powered conversations about Charlotte third places.
Provides streaming chat endpoint with RAG context from Cosmos DB.
"""

import json
import logging
import azure.functions as func
import azure.durable_functions as df
from typing import Optional

bp = df.Blueprint()


@bp.function_name(name="Chat")
@bp.route(route="chat", methods=["POST"])
def chat(req: func.HttpRequest) -> func.HttpResponse:
    """
    Chat endpoint for AI-powered conversations about third places.
    
    Accepts POST with JSON body:
    {
        "messages": [{"role": "user"|"assistant", "content": "..."}],
        "placeId": "optional-place-id-for-context",
        "stream": true|false
    }
    
    Returns streaming or non-streaming response based on stream parameter.
    """
    logging.info("Chat endpoint called")

    try:
        # Parse request body
        try:
            body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Invalid JSON body",
                    "error": "Request body must be valid JSON"
                }),
                status_code=400,
                mimetype="application/json"
            )

        messages = body.get("messages", [])
        place_id: Optional[str] = body.get("placeId")
        stream = body.get("stream", False)

        if not messages:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required field: messages",
                    "error": "messages array is required"
                }),
                status_code=400,
                mimetype="application/json"
            )

        # Get the latest user message for vector search
        user_messages = [m for m in messages if m.get("role") == "user"]
        if not user_messages:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "No user messages found",
                    "error": "At least one user message is required"
                }),
                status_code=400,
                mimetype="application/json"
            )

        latest_query = user_messages[-1].get("content", "")

        # Initialize services
        from services.embedding_service import EmbeddingService
        from services.cosmos_service import CosmosService
        from services.chat_service import ChatService

        embedding_service = EmbeddingService()
        cosmos_service = CosmosService()
        chat_service = ChatService()

        # Generate embedding for the query
        query_embedding = embedding_service.get_embedding(latest_query)

        # Perform vector search
        place_context = None
        if place_id:
            # Get specific place context and search its reviews
            place_context = cosmos_service.get_place(place_id)
            chunks = cosmos_service.vector_search_chunks(
                query_embedding=query_embedding,
                top_k=8,
                min_score=0.65,
                place_id=place_id
            )
            # Also get related places for broader context
            places = cosmos_service.vector_search_places(
                query_embedding=query_embedding,
                top_k=3,
                min_score=0.7
            )
        else:
            # General search across all places and reviews
            places = cosmos_service.vector_search_places(
                query_embedding=query_embedding,
                top_k=5,
                min_score=0.7
            )
            chunks = cosmos_service.vector_search_chunks(
                query_embedding=query_embedding,
                top_k=10,
                min_score=0.65
            )

        # Create context for RAG
        context = chat_service.create_context_message(
            places=places,
            chunks=chunks,
            place_context=place_context
        )

        logging.info(f"RAG context created with {len(places)} places and {len(chunks)} chunks")

        if stream:
            # Return streaming response
            def generate():
                for chunk in chat_service.chat(messages, context, stream=True):
                    # Format as Server-Sent Events
                    yield f"data: {json.dumps({'content': chunk})}\n\n"
                yield "data: [DONE]\n\n"

            return func.HttpResponse(
                body=None,
                status_code=200,
                mimetype="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no"
                }
            )
            # Note: Azure Functions doesn't support true streaming responses well.
            # We'll return the full response for now and handle streaming in the Next.js proxy.
        
        # Non-streaming response
        response_content = chat_service.chat(messages, context, stream=False)

        return func.HttpResponse(
            json.dumps({
                "success": True,
                "message": "Chat response generated",
                "data": {
                    "content": response_content,
                    "context": {
                        "placesCount": len(places),
                        "chunksCount": len(chunks),
                        "placeId": place_id
                    }
                }
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as ex:
        logging.error(f"Error in chat endpoint: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while processing chat request",
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )


@bp.function_name(name="ChatStream")
@bp.route(route="chat/stream", methods=["POST"])
def chat_stream(req: func.HttpRequest) -> func.HttpResponse:
    """
    Streaming chat endpoint that returns chunked responses.
    
    This endpoint is designed for streaming responses, returning
    newline-delimited JSON chunks that the client can process incrementally.
    
    Accepts POST with JSON body:
    {
        "messages": [{"role": "user"|"assistant", "content": "..."}],
        "placeId": "optional-place-id-for-context"
    }
    """
    logging.info("Chat stream endpoint called")

    try:
        # Parse request body
        try:
            body = req.get_json()
        except ValueError:
            return func.HttpResponse(
                json.dumps({"error": "Invalid JSON body"}),
                status_code=400,
                mimetype="application/json"
            )

        messages = body.get("messages", [])
        place_id: Optional[str] = body.get("placeId")

        if not messages:
            return func.HttpResponse(
                json.dumps({"error": "messages array is required"}),
                status_code=400,
                mimetype="application/json"
            )

        # Get the latest user message for vector search
        user_messages = [m for m in messages if m.get("role") == "user"]
        if not user_messages:
            return func.HttpResponse(
                json.dumps({"error": "At least one user message is required"}),
                status_code=400,
                mimetype="application/json"
            )

        latest_query = user_messages[-1].get("content", "")

        # Initialize services
        from services.embedding_service import EmbeddingService
        from services.cosmos_service import CosmosService
        from services.chat_service import ChatService

        embedding_service = EmbeddingService()
        cosmos_service = CosmosService()
        chat_service = ChatService()

        # Generate embedding for the query
        query_embedding = embedding_service.get_embedding(latest_query)

        # Perform vector search
        place_context = None
        if place_id:
            place_context = cosmos_service.get_place(place_id)
            chunks = cosmos_service.vector_search_chunks(
                query_embedding=query_embedding,
                top_k=8,
                min_score=0.65,
                place_id=place_id
            )
            places = cosmos_service.vector_search_places(
                query_embedding=query_embedding,
                top_k=3,
                min_score=0.7
            )
        else:
            places = cosmos_service.vector_search_places(
                query_embedding=query_embedding,
                top_k=5,
                min_score=0.7
            )
            chunks = cosmos_service.vector_search_chunks(
                query_embedding=query_embedding,
                top_k=10,
                min_score=0.65
            )

        # Create context for RAG
        context = chat_service.create_context_message(
            places=places,
            chunks=chunks,
            place_context=place_context
        )

        logging.info(f"RAG context created with {len(places)} places and {len(chunks)} chunks")

        # Collect streaming response into a single response
        # Azure Functions HTTP trigger doesn't support true streaming,
        # so we collect and return newline-delimited JSON
        response_chunks = []
        for chunk in chat_service.chat(messages, context, stream=True):
            response_chunks.append(chunk)

        # Return as newline-delimited JSON for easier client parsing
        full_content = "".join(response_chunks)
        
        return func.HttpResponse(
            json.dumps({
                "content": full_content,
                "context": {
                    "placesCount": len(places),
                    "chunksCount": len(chunks),
                    "placeId": place_id
                }
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as ex:
        logging.error(f"Error in chat stream endpoint: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": str(ex)}),
            status_code=500,
            mimetype="application/json"
        )
