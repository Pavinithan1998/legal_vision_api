"""
Legal Router
Handles statute, section, and legal principle queries
"""

from fastapi import APIRouter, HTTPException, Query, Path
from typing import List

from app.models.schemas import (
    StatuteResponse, SectionResponse, PrincipleResponse,
    LegalQueryRequest, DeedType
)
from app.services.graph_service import graph_service
from app.services.llm_service import llm_service
from app.utils.intent_detection import QueryType


router = APIRouter()


# =============================================================================
# STATUTES
# =============================================================================

@router.get("/statutes", response_model=List[dict])
async def list_statutes():
    """
    List all Sri Lankan property law statutes in the knowledge graph.
    """
    statutes = graph_service.get_all_statutes()
    return statutes


@router.get("/statutes/search")
async def search_statutes(
    q: str = Query(..., description="Search query", min_length=2)
):
    """
    Search statutes by name, short name, or category.
    
    **Examples:**
    - "Prevention of Frauds"
    - "Registration"
    - "Mortgage"
    """
    results = graph_service.search_statutes(q)
    
    if not results:
        return {
            "query": q,
            "count": 0,
            "statutes": [],
            "suggestion": "Try searching for: Prevention of Frauds, Registration, Mortgage, Partition, Prescription"
        }
    
    return {
        "query": q,
        "count": len(results),
        "statutes": results
    }


@router.get("/statutes/for-deed-type/{deed_type}")
async def get_statutes_for_deed_type(
    deed_type: DeedType = Path(..., description="Type of deed")
):
    """
    Get all statutes applicable to a specific deed type.
    
    Returns statutes with their critical sections.
    """
    results = graph_service.get_statutes_for_deed_type(deed_type.value)
    
    return {
        "deed_type": deed_type.value,
        "count": len(results),
        "statutes": results
    }


# =============================================================================
# SECTIONS
# =============================================================================

@router.get("/sections/search")
async def search_sections(
    q: str = Query(..., description="Search query", min_length=2)
):
    """
    Search statute sections by title, content, or section number.
    
    **Examples:**
    - "Section 2"
    - "notarially"
    - "registration"
    """
    results = graph_service.search_sections(q)
    
    return {
        "query": q,
        "count": len(results),
        "sections": results
    }


# =============================================================================
# LEGAL PRINCIPLES
# =============================================================================

@router.get("/principles", response_model=List[dict])
async def list_principles():
    """
    List all legal principles in the knowledge graph.
    
    Returns principles like Nemo Dat, Caveat Emptor, etc.
    """
    principles = graph_service.get_all_principles()
    return principles


@router.get("/principles/search")
async def search_principles(
    q: str = Query(..., description="Search query", min_length=2)
):
    """
    Search legal principles by name, meaning, or description.
    
    **Examples:**
    - "Nemo Dat"
    - "Caveat"
    - "possession"
    """
    results = graph_service.search_principles(q)
    
    return {
        "query": q,
        "count": len(results),
        "principles": results
    }


# =============================================================================
# DEED REQUIREMENTS
# =============================================================================

@router.get("/requirements/{deed_type}")
async def get_deed_requirements(
    deed_type: DeedType = Path(..., description="Type of deed")
):
    """
    Get legal requirements for a specific deed type.
    
    Returns:
    - Required items (written document, notarization, etc.)
    - Stamp duty rules
    - Registration fees
    - Governing statutes
    """
    results = graph_service.get_deed_requirements(deed_type.value)
    
    if not results:
        return {
            "deed_type": deed_type.value,
            "found": False,
            "message": f"No requirements found for {deed_type.value}"
        }
    
    return {
        "deed_type": deed_type.value,
        "found": True,
        "requirements": results
    }


# =============================================================================
# LEGAL REASONING
# =============================================================================

@router.post("/reason")
async def legal_reasoning(request: LegalQueryRequest):
    """
    Get legal reasoning for a question using IRAC format.
    
    Provides:
    - Issue identification
    - Rule citation (statutes/sections)
    - Application to facts
    - Conclusion
    
    **Examples:**
    - "What are the requirements for a valid sale deed?"
    - "Is a deed valid if not registered within 3 months?"
    - "Can a foreigner own freehold land in Sri Lanka?"
    """
    # Gather relevant legal data
    graph_data = []
    
    # Get deed data if code provided
    if request.deed_code:
        deed = graph_service.get_deed_by_code(request.deed_code)
        if deed:
            graph_data.append(deed)
        
        law_data = graph_service.get_governing_law(request.deed_code)
        if law_data:
            graph_data.append(law_data)
    
    # Get requirements if deed type provided
    if request.deed_type:
        requirements = graph_service.get_deed_requirements(request.deed_type.value)
        graph_data.extend(requirements)
        
        statutes = graph_service.get_statutes_for_deed_type(request.deed_type.value)
        graph_data.extend(statutes)
    
    # Search for relevant statutes based on question
    statute_results = graph_service.search_statutes(request.question)
    graph_data.extend(statute_results[:3])
    
    # Search for relevant principles
    principle_results = graph_service.search_principles(request.question)
    graph_data.extend(principle_results[:2])
    
    # Generate reasoned response
    llm_response = llm_service.generate_response(
        user_query=request.question,
        graph_data=graph_data,
        intent="legal_reasoning",
        query_type=QueryType.LEGAL_REASONING,
        include_reasoning=True
    )
    
    return {
        "question": request.question,
        "deed_code": request.deed_code,
        "deed_type": request.deed_type.value if request.deed_type else None,
        "answer": llm_response["answer"],
        "irac_analysis": llm_response.get("irac_analysis"),
        "reasoning_steps": llm_response.get("reasoning_steps"),
        "referenced_statutes": llm_response.get("related_statutes", []),
        "confidence": llm_response.get("confidence", 0.5)
    }


@router.get("/explain/{term}")
async def explain_legal_concept(
    term: str = Path(..., description="Legal term to explain")
):
    """
    Get an explanation of a legal concept with relevant statutes.
    
    **Examples:**
    - "prescription"
    - "caveat emptor"
    - "easement"
    """
    # Get definition
    definitions = graph_service.search_definitions(term)
    
    # Get principles
    principles = graph_service.search_principles(term)
    
    # Get related statutes
    statutes = graph_service.search_statutes(term)
    
    # Combine data
    graph_data = definitions + principles + statutes
    
    if not graph_data:
        raise HTTPException(
            status_code=404, 
            detail=f"No information found for '{term}'. Try: prescription, mortgage, easement, conveyance"
        )
    
    # Generate explanation
    llm_response = llm_service.generate_response(
        user_query=f"Explain the legal concept of '{term}' in Sri Lankan property law",
        graph_data=graph_data,
        intent="explain_concept",
        query_type=QueryType.DEFINITION,
        include_reasoning=False
    )
    
    return {
        "term": term,
        "explanation": llm_response["answer"],
        "definitions": definitions,
        "principles": principles,
        "related_statutes": [s.get('statute_name') for s in statutes if s.get('statute_name')]
    }
