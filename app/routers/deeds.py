"""
Deeds Router
Handles deed-specific queries and operations
"""

from fastapi import APIRouter, HTTPException, Query, Path
from typing import Optional, List

from app.models.schemas import (
    DeedResponse, DeedSearchRequest, DeedType,
    Party, Property, Boundaries
)
from app.services.graph_service import graph_service


router = APIRouter()


@router.get("/{deed_code}", response_model=DeedResponse)
async def get_deed(
    deed_code: str = Path(..., description="Deed code (e.g., 'A 1100/188')")
):
    """
    Get full details of a specific deed.
    
    Returns comprehensive information including parties, property,
    boundaries, governing statutes, and prior deed references.
    """
    deed = graph_service.get_deed_by_code(deed_code)
    
    if not deed:
        raise HTTPException(status_code=404, detail=f"Deed {deed_code} not found")
    
    # Build response
    boundaries = None
    if any([deed.get('north'), deed.get('south'), deed.get('east'), deed.get('west')]):
        boundaries = Boundaries(
            north=deed.get('north'),
            south=deed.get('south'),
            east=deed.get('east'),
            west=deed.get('west')
        )
    
    property_info = Property(
        lot=deed.get('lot'),
        extent=deed.get('extent'),
        assessment_no=deed.get('assessment'),
        plan_no=deed.get('plan_no'),
        plan_date=deed.get('plan_date'),
        boundaries=boundaries
    )
    
    parties = []
    for p in deed.get('parties', []):
        if isinstance(p, dict):
            parties.append(Party(
                name=p.get('name', 'Unknown'),
                role=p.get('role', 'Unknown')
            ))
    
    return DeedResponse(
        deed_code=deed.get('deed_code', deed_code),
        deed_type=deed.get('deed_type', 'unknown'),
        date=deed.get('date'),
        district=deed.get('district'),
        province=deed.get('province'),
        registry=deed.get('registry'),
        amount=deed.get('amount'),
        property=property_info,
        parties=parties,
        prior_deed=deed.get('prior_deed'),
        governing_statutes=deed.get('governing_statutes', [])
    )


@router.get("/{deed_code}/parties")
async def get_deed_parties(
    deed_code: str = Path(..., description="Deed code")
):
    """
    Get all parties involved in a deed.
    
    Returns vendors, vendees, notaries, witnesses, etc.
    """
    parties = graph_service.get_deed_parties(deed_code)
    
    if not parties:
        raise HTTPException(status_code=404, detail=f"No parties found for deed {deed_code}")
    
    return {
        "deed_code": deed_code,
        "parties": [
            {
                "name": p.get('person_name'),
                "role": p.get('role'),
                "nic": p.get('nic')
            }
            for p in parties
        ]
    }


@router.get("/{deed_code}/boundaries")
async def get_deed_boundaries(
    deed_code: str = Path(..., description="Deed code")
):
    """
    Get property boundaries for a deed.
    
    Returns North, South, East, West boundaries with adjacent property references.
    """
    boundary_data = graph_service.get_deed_boundaries(deed_code)
    
    if not boundary_data:
        raise HTTPException(status_code=404, detail=f"No boundary data found for deed {deed_code}")
    
    return {
        "deed_code": boundary_data.get('deed_code', deed_code),
        "lot": boundary_data.get('lot'),
        "extent": boundary_data.get('extent'),
        "boundaries": {
            "north": boundary_data.get('north'),
            "south": boundary_data.get('south'),
            "east": boundary_data.get('east'),
            "west": boundary_data.get('west')
        }
    }


@router.get("/{deed_code}/history")
async def get_ownership_history(
    deed_code: str = Path(..., description="Deed code")
):
    """
    Get ownership chain/history for a deed.
    
    Traces prior deed references to show property ownership history.
    """
    chain = graph_service.get_ownership_chain(deed_code)
    
    if not chain:
        raise HTTPException(status_code=404, detail=f"No ownership history found for deed {deed_code}")
    
    return {
        "deed_code": deed_code,
        "chain": [
            {
                "deed": c.get('current_deed'),
                "type": c.get('deed_type'),
                "date": c.get('date'),
                "prior_reference": c.get('prior_reference'),
                "prior_deed": c.get('prior_deed_code'),
                "parties": c.get('current_parties', [])
            }
            for c in chain
        ]
    }


@router.get("/{deed_code}/governing-law")
async def get_governing_law(
    deed_code: str = Path(..., description="Deed code")
):
    """
    Get governing statutes and legal requirements for a deed.
    """
    law_data = graph_service.get_governing_law(deed_code)
    
    if not law_data:
        raise HTTPException(status_code=404, detail=f"No governing law found for deed {deed_code}")
    
    return {
        "deed_code": law_data.get('deed_code', deed_code),
        "deed_type": law_data.get('deed_type'),
        "governing_statutes": law_data.get('governing_statutes', []),
        "requirements": law_data.get('requirements', []),
        "stamp_duty": law_data.get('stamp_duty'),
        "critical_sections": law_data.get('critical_sections', [])
    }


@router.post("/search")
async def search_deeds(request: DeedSearchRequest):
    """
    Search for deeds using various criteria.
    """
    results = []
    
    if request.code:
        deed = graph_service.get_deed_by_code(request.code)
        if deed:
            results = [deed]
    elif request.person_name:
        results = graph_service.search_deeds_by_person(request.person_name, request.limit)
    elif request.district:
        results = graph_service.search_deeds_by_district(request.district, request.limit)
    elif request.deed_type:
        results = graph_service.search_deeds_by_type(request.deed_type.value, request.limit)
    else:
        # Return recent deeds as default
        results = graph_service.get_recent_deeds(request.limit)
    
    return {
        "query": request.dict(exclude_none=True),
        "count": len(results),
        "deeds": results
    }


@router.get("/by-person/{name}")
async def get_deeds_by_person(
    name: str = Path(..., description="Person name to search"),
    limit: int = Query(10, ge=1, le=50)
):
    """Search deeds by person name."""
    results = graph_service.search_deeds_by_person(name, limit)
    return {
        "person": name,
        "count": len(results),
        "deeds": results
    }


@router.get("/by-district/{district}")
async def get_deeds_by_district(
    district: str = Path(..., description="District name"),
    limit: int = Query(10, ge=1, le=50)
):
    """Search deeds by district."""
    results = graph_service.search_deeds_by_district(district, limit)
    return {
        "district": district,
        "count": len(results),
        "deeds": results
    }


@router.get("/by-type/{deed_type}")
async def get_deeds_by_type(
    deed_type: DeedType = Path(..., description="Type of deed"),
    limit: int = Query(10, ge=1, le=50)
):
    """Search deeds by type."""
    results = graph_service.search_deeds_by_type(deed_type.value, limit)
    return {
        "deed_type": deed_type.value,
        "count": len(results),
        "deeds": results
    }


@router.get("/by-boundary/{name}")
async def get_deeds_by_boundary(
    name: str = Path(..., description="Name in boundary reference"),
):
    """Search deeds by boundary reference (adjacent property owner)."""
    results = graph_service.search_deeds_by_boundary(name)
    return {
        "boundary_reference": name,
        "count": len(results),
        "deeds": results
    }


@router.get("/recent/")
async def get_recent_deeds(
    limit: int = Query(10, ge=1, le=50)
):
    """Get most recent deeds."""
    results = graph_service.get_recent_deeds(limit)
    return {
        "count": len(results),
        "deeds": results
    }


@router.get("/highest-value/")
async def get_highest_value_deeds(
    limit: int = Query(10, ge=1, le=50)
):
    """Get highest value deeds by consideration amount."""
    results = graph_service.get_highest_value_deeds(limit)
    return {
        "count": len(results),
        "deeds": results
    }
