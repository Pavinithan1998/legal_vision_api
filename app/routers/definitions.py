"""
Definitions Router
Handles legal term definitions and terminology queries
"""

from fastapi import APIRouter, HTTPException, Query, Path
from typing import List

from app.models.schemas import DefinitionResponse
from app.services.graph_service import graph_service


router = APIRouter()


@router.get("/", response_model=List[dict])
async def list_definitions():
    """
    List all legal definitions in the knowledge graph.
    
    Returns definitions for terms like:
    - Immovable Property
    - Conveyance
    - Vendor/Vendee
    - Mortgage
    - Prescription
    - etc.
    """
    definitions = graph_service.get_all_definitions()
    return definitions


@router.get("/search")
async def search_definitions(
    q: str = Query(..., description="Search term", min_length=2)
):
    """
    Search legal definitions by term.
    
    **Examples:**
    - "mortgage"
    - "vendor"
    - "prescription"
    - "conveyance"
    """
    results = graph_service.search_definitions(q)
    
    if not results:
        # Get all definitions for suggestions
        all_defs = graph_service.get_all_definitions()
        terms = [d.get('term') for d in all_defs if d.get('term')]
        
        return {
            "query": q,
            "count": 0,
            "definitions": [],
            "suggestions": terms[:10]
        }
    
    return {
        "query": q,
        "count": len(results),
        "definitions": results
    }


@router.get("/term/{term}")
async def get_definition(
    term: str = Path(..., description="Legal term to define")
):
    """
    Get the definition of a specific legal term.
    
    Returns the definition and its source.
    """
    results = graph_service.search_definitions(term)
    
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"Definition not found for '{term}'"
        )
    
    # Return the best match (first result)
    definition = results[0]
    
    return {
        "term": definition.get('term'),
        "definition": definition.get('definition'),
        "source": definition.get('source'),
        "related_terms": [r.get('term') for r in results[1:4]] if len(results) > 1 else []
    }


@router.get("/categories")
async def get_definition_categories():
    """
    Get definitions grouped by category/source.
    """
    all_defs = graph_service.get_all_definitions()
    
    # Group by source
    by_source = {}
    for d in all_defs:
        source = d.get('source', 'Unknown')
        if source not in by_source:
            by_source[source] = []
        by_source[source].append(d.get('term'))
    
    return {
        "total": len(all_defs),
        "by_source": by_source
    }
