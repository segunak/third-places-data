"""
Chat service for AI-powered conversations about Charlotte's third places.
"""

import os
import logging
from typing import List, Dict, Any, Optional, Generator
from openai import AzureOpenAI

# Configure logging
logger = logging.getLogger(__name__)

# System prompt for the friendly local guide persona
SYSTEM_PROMPT = """You are a friendly, knowledgeable local guide for Charlotte, North Carolina, specializing in "third places" - those wonderful spots that aren't home or work where people go to study, read, write, work remotely, relax, or socialize.

Your personality:
- Warm and welcoming, like a friend who knows all the best spots in town
- Enthusiastic about Charlotte's diverse neighborhoods and local businesses
- Helpful and specific in your recommendations
- Honest about limitations - if a place might not suit someone's needs, say so kindly

Your knowledge:
- You have detailed information about coffee shops, cafes, libraries, bookstores, bubble tea shops, breweries, and other third places in Charlotte
- You know about amenities like Wi-Fi, parking, seating, noise levels, and purchase requirements
- You understand what makes different places suitable for different activities (studying vs. socializing vs. remote work)
- You're familiar with Charlotte's neighborhoods and can suggest places based on location

Guidelines:
- Always base recommendations on the context provided about specific places
- If asked about a place not in your context, acknowledge you don't have specific information about it
- Provide practical details: hours, parking, Wi-Fi availability when known
- Suggest alternatives when a specific place might not meet someone's needs
- Keep responses conversational but informative
- If someone asks about something unrelated to third places or Charlotte, gently redirect the conversation

When discussing a specific place, mention:
- What makes it special or suitable for their needs
- Practical details (neighborhood, parking, Wi-Fi if known)
- Any caveats or considerations
- Similar alternatives if relevant

Remember: You're here to help people find their perfect spot in Charlotte!"""


class ChatService:
    """Service for AI chat using Microsoft Foundry."""

    def __init__(self):
        """Initialize the chat service with Microsoft Foundry credentials."""
        self.api_key = os.environ.get("FOUNDRY_API_KEY")
        if not self.api_key:
            raise ValueError("FOUNDRY_API_KEY environment variable is required")

        self.endpoint = "https://foundry-third-places.services.ai.azure.com/"
        self.model = "gpt-5-mini"
        self.max_tokens = 1024
        self.temperature = 0.7

        # Initialize Azure OpenAI client
        self.client = AzureOpenAI(
            api_key=self.api_key,
            api_version="2024-10-21",
            azure_endpoint=self.endpoint
        )

        logger.info(f"ChatService initialized with endpoint: {self.endpoint}")

    def create_context_message(
        self,
        places: List[Dict[str, Any]],
        chunks: List[Dict[str, Any]],
        place_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Create a context message from retrieved places and chunks for RAG.
        
        Args:
            places: List of place documents from vector search.
            chunks: List of chunk (review) documents from vector search.
            place_context: Optional specific place context when chatting about one place.
            
        Returns:
            Formatted context string for the assistant.
        """
        context_parts = []

        # Add specific place context if provided (for place-specific chats)
        if place_context:
            context_parts.append("=== Current Place Being Discussed ===")
            context_parts.append(self._format_place(place_context))
            context_parts.append("")

        # Add relevant places from vector search
        if places:
            context_parts.append("=== Relevant Places ===")
            for i, place in enumerate(places, 1):
                context_parts.append(f"--- Place {i} (Relevance: {place.get('similarityScore', 'N/A')}) ---")
                context_parts.append(self._format_place(place))
                context_parts.append("")

        # Add relevant reviews from vector search
        if chunks:
            context_parts.append("=== Relevant Reviews ===")
            for i, chunk in enumerate(chunks, 1):
                context_parts.append(f"--- Review {i} (Relevance: {chunk.get('similarityScore', 'N/A')}) ---")
                context_parts.append(self._format_chunk(chunk))
                context_parts.append("")

        if not context_parts:
            return "No specific place information available for this query."

        return "\n".join(context_parts)

    def _format_place(self, place: Dict[str, Any]) -> str:
        """Format a place document for context."""
        lines = []
        
        if place.get("place"):
            lines.append(f"Name: {place['place']}")
        if place.get("type"):
            lines.append(f"Type: {place['type']}")
        if place.get("neighborhood"):
            lines.append(f"Neighborhood: {place['neighborhood']}")
        if place.get("address"):
            lines.append(f"Address: {place['address']}")
        if place.get("description"):
            lines.append(f"Description: {place['description']}")
        if place.get("tags"):
            tags = place['tags'] if isinstance(place['tags'], list) else [place['tags']]
            lines.append(f"Tags: {', '.join(tags)}")
        if place.get("freeWifi") is not None:
            lines.append(f"Free Wi-Fi: {'Yes' if place['freeWifi'] else 'No'}")
        if place.get("parking"):
            lines.append(f"Parking: {place['parking']}")
        if place.get("size"):
            lines.append(f"Size: {place['size']}")
        if place.get("purchaseRequired") is not None:
            lines.append(f"Purchase Required: {'Yes' if place['purchaseRequired'] else 'No'}")
        if place.get("placeRating"):
            lines.append(f"Rating: {place['placeRating']}/5")
        if place.get("reviewsCount"):
            lines.append(f"Number of Reviews: {place['reviewsCount']}")
        if place.get("typicalTimeSpent"):
            lines.append(f"Typical Time Spent: {place['typicalTimeSpent']}")
        if place.get("workingHours"):
            hours = place['workingHours']
            if isinstance(hours, dict):
                hours_str = ", ".join([f"{day}: {time}" for day, time in hours.items()])
                lines.append(f"Hours: {hours_str}")
        if place.get("about"):
            about = place['about']
            if isinstance(about, dict):
                about_items = [f"{k}: {v}" for k, v in about.items() if v]
                if about_items:
                    lines.append(f"About: {'; '.join(about_items)}")
        
        return "\n".join(lines)

    def _format_chunk(self, chunk: Dict[str, Any]) -> str:
        """Format a chunk (review) document for context."""
        lines = []
        
        if chunk.get("placeName"):
            lines.append(f"Place: {chunk['placeName']}")
        if chunk.get("neighborhood"):
            lines.append(f"Neighborhood: {chunk['neighborhood']}")
        if chunk.get("reviewText"):
            lines.append(f"Review: {chunk['reviewText']}")
        if chunk.get("reviewRating"):
            lines.append(f"Rating: {chunk['reviewRating']}/5")
        if chunk.get("ownerAnswer"):
            lines.append(f"Owner Response: {chunk['ownerAnswer']}")
        
        return "\n".join(lines)

    def chat(
        self,
        messages: List[Dict[str, str]],
        context: str,
        stream: bool = False
    ) -> Generator[str, None, None] | str:
        """
        Generate a chat response with RAG context.
        
        Args:
            messages: List of message dicts with 'role' and 'content'.
            context: RAG context string from create_context_message.
            stream: Whether to stream the response.
            
        Returns:
            If stream=False: Complete response string.
            If stream=True: Generator yielding response chunks.
        """
        # Build messages array with system prompt and context
        full_messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "system", "content": f"Here is relevant information about Charlotte third places to help answer the user's question:\n\n{context}"}
        ]
        
        # Add conversation history
        full_messages.extend(messages)

        logger.info(f"Sending chat request with {len(messages)} user messages")

        if stream:
            return self._stream_response(full_messages)
        else:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=full_messages,
                max_tokens=self.max_tokens,
                temperature=self.temperature
            )
            return response.choices[0].message.content

    def _stream_response(self, messages: List[Dict[str, str]]) -> Generator[str, None, None]:
        """Stream chat response chunks."""
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=self.max_tokens,
            temperature=self.temperature,
            stream=True
        )

        for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
