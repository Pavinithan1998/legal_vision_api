"""
Query Router
Handles natural language queries with full reasoning pipeline
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import uuid
import logging

from app.models.schemas import QueryRequest, QueryResponse, StatsResponse
from app.services.graph_service import graph_service
from app.services.llm_service import llm_service
from app.services.session_manager import session_manager
from app.utils.intent_detection import intent_detector

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest):
    """
    Process a natural language query about Sri Lankan property law.
    
    This endpoint:
    1. Detects the intent and extracts entities from the query
    2. Retrieves relevant data from the knowledge graph
    3. Generates a response using GPT-4o with legal reasoning
    4. Returns structured response with IRAC analysis if applicable
    
    **Examples:**
    - "What laws govern sale deeds in Sri Lanka?"
    - "Show details of deed A 1100/188"
    - "Is this deed valid?" (follow-up)
    - "What is prescription in property law?"
    """
    try:
        # Get or create session
        session_id = request.session_id or str(uuid.uuid4())
        logger.info(f"Processing query with session {session_id}")
        
        context = session_manager.get_context(session_id)
        
        # Detect intent
        logger.info(f"Detecting intent for: {request.query[:50]}...")
        intent, params, query_type = intent_detector.detect_intent(
            request.query, 
            context
        )
        logger.info(f"Detected intent: {intent.value}")
        
        # Execute graph query
        logger.info(f"Executing graph query with params: {params}")
        graph_data = graph_service.execute_intent(intent, params)
        logger.info(f"Retrieved {len(graph_data)} records from graph")
        
        # Update session context
        session_manager.update_context(session_id, intent.value, params, graph_data)
        
        # Generate response with LLM
        logger.info("Generating LLM response...")
        llm_response = llm_service.generate_response(
            user_query=request.query,
            graph_data=graph_data,
            intent=intent.value,
            query_type=query_type,
            conversation_history=session_manager.get_history(session_id),
            include_reasoning=request.include_reasoning
        )
        logger.info("LLM response generated successfully")
        
        # Add to history
        session_manager.add_to_history(
            session_id=session_id,
            query=request.query,
            intent=intent.value,
            params=params,
            results_count=len(graph_data),
            response_summary=llm_response["answer"][:100] if llm_response["answer"] else None
        )
        
        return QueryResponse(
            query=request.query,
            intent=intent.value,
            query_type=query_type.value,
            answer=llm_response["answer"],
            reasoning_steps=llm_response.get("reasoning_steps"),
            irac_analysis=llm_response.get("irac_analysis"),
            sources=llm_response.get("sources", []),
            related_statutes=llm_response.get("related_statutes", []),
            confidence=llm_response.get("confidence", 0.5),
            data={"results_count": len(graph_data), "session_id": session_id}
        )
    
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")


@router.get("/stats", response_model=StatsResponse)
async def get_statistics():
    """
    Get knowledge graph statistics.
    
    Returns counts of deeds, persons, properties, statutes, and definitions.
    """
    stats = graph_service.get_statistics()
    return StatsResponse(**stats)


@router.post("/session/{session_id}/clear")
async def clear_session(session_id: str):
    """Clear a session's context and history."""
    session_manager.clear_session(session_id)
    return {"message": f"Session {session_id} cleared"}


@router.get("/session/{session_id}/context")
async def get_session_context(session_id: str):
    """Get current context for a session."""
    context = session_manager.get_context(session_id)
    history = session_manager.get_history(session_id)
    return {
        "session_id": session_id,
        "context": context,
        "history_count": len(history),
        "recent_queries": [h["query"] for h in history[-3:]]
    }


@router.get("/search")
async def general_search(
    q: str = Query(..., description="Search query", min_length=2)
):
    """
    Perform a general search across all entity types.
    
    Searches: Persons, Deeds, Districts, Properties, Statutes, Definitions, Principles
    """
    results = graph_service.general_search(q)
    
    # Group by type
    grouped = {}
    for record in results:
        entity_type = record.get("type", "Unknown")
        if entity_type not in grouped:
            grouped[entity_type] = []
        grouped[entity_type].append({
            "name": record.get("name"),
            "code": record.get("code"),
            "extra": record.get("extra")
        })
    
    return {
        "query": q,
        "total_results": len(results),
        "results_by_type": grouped
    }
