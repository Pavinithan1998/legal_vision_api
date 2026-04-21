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
from app.utils.cypher_queries import queries


router = APIRouter()


# =============================================================================
# STATIC / NON-PARAMETERISED ROUTES
# Must all come BEFORE /{deed_code} — otherwise FastAPI matches them as a code.
# =============================================================================

@router.get("/graph/data", summary="Nodes + edges for the Knowledge Graph visualisation")
async def get_graph_data(
    limit: int = Query(50, ge=1, le=200, description="Max instruments to include")
):
    """
    Returns the full graph payload consumed by the Knowledge Graph page.

    Shape:
    {
        "nodes": [{"id", "label", "type", ...props}],
        "edges": [{"source", "target", "label"}]
    }

    Node types: Document, Person, Property, Location
    Edge labels: vendor, vendee, donor, donee, notary …, documents, located in, derives from
    """
    results = graph_service.driver.execute_query(
        queries.GET_GRAPH_DATA,
        {"limit": limit}
    )

    nodes: list = []
    edges: list = []
    seen: set = set()

    def add_node(node_id: str, label: str, node_type: str, props: dict = None):
        if node_id and node_id not in seen:
            seen.add(node_id)
            nodes.append({
                "id": node_id,
                "label": label,
                "type": node_type,
                **(props or {}),
            })

    for row in results:
        deed_id = row.get("deed_code")
        if not deed_id:
            continue

        # ── Instrument node ──────────────────────────────────────────
        add_node(deed_id, deed_id, "Document", {
            "deed_type": row.get("deed_type"),
            "date": row.get("date"),
        })

        # ── Person nodes + edges ─────────────────────────────────────
        for person in (row.get("persons") or []):
            name = person.get("name")
            role = person.get("role") or "involved in"
            if name:
                add_node(name, name, "Person")
                edges.append({
                    "source": name,
                    "target": deed_id,
                    "label": role,
                })

        # ── Property node + edge ─────────────────────────────────────
        lot = row.get("lot")
        if lot:
            prop_id = f"lot_{lot}"
            add_node(prop_id, f"Lot {lot}", "Property")
            edges.append({
                "source": deed_id,
                "target": prop_id,
                "label": "documents",
            })

        # ── District / Location node + edge ──────────────────────────
        district = row.get("district")
        if district:
            add_node(district, district, "Location")
            edges.append({
                "source": deed_id,
                "target": district,
                "label": "located in",
            })

        # ── Prior deed chain edge ─────────────────────────────────────
        prior = row.get("prior_deed_code")
        if prior:
            edges.append({
                "source": deed_id,
                "target": prior,
                "label": "derives from",
            })

    return {"nodes": nodes, "edges": edges}


@router.get("/recent/", summary="Get most recent deeds")
async def get_recent_deeds(
    limit: int = Query(10, ge=1, le=50)
):
    """Get most recently dated deeds."""
    results = graph_service.get_recent_deeds(limit)
    return {
        "count": len(results),
        "deeds": results,
    }


@router.get("/highest-value/", summary="Get highest value deeds")
async def get_highest_value_deeds(
    limit: int = Query(10, ge=1, le=50)
):
    """Get highest value deeds by consideration amount."""
    results = graph_service.get_highest_value_deeds(limit)
    return {
        "count": len(results),
        "deeds": results,
    }


@router.post("/search", summary="Search deeds by various criteria")
async def search_deeds(request: DeedSearchRequest):
    """
    Search for deeds using various criteria.
    Falls back to recent deeds when no filter is supplied.
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
        results = graph_service.get_recent_deeds(request.limit)

    return {
        "query": request.dict(exclude_none=True),
        "count": len(results),
        "deeds": results,
    }


@router.get("/by-person/{name}", summary="Search deeds by person name")
async def get_deeds_by_person(
    name: str = Path(..., description="Person name to search"),
    limit: int = Query(10, ge=1, le=50)
):
    """Search deeds by person name (vendor, vendee, notary, etc.)."""
    results = graph_service.search_deeds_by_person(name, limit)
    return {
        "person": name,
        "count": len(results),
        "deeds": results,
    }


@router.get("/by-district/{district}", summary="Search deeds by district")
async def get_deeds_by_district(
    district: str = Path(..., description="District name"),
    limit: int = Query(10, ge=1, le=50)
):
    """Search deeds by district."""
    results = graph_service.search_deeds_by_district(district, limit)
    return {
        "district": district,
        "count": len(results),
        "deeds": results,
    }


@router.get("/by-type/{deed_type}", summary="Search deeds by type")
async def get_deeds_by_type(
    deed_type: DeedType = Path(..., description="Type of deed"),
    limit: int = Query(10, ge=1, le=50)
):
    """Search deeds by type (sale_transfer, gift, will, lease, mortgage)."""
    results = graph_service.search_deeds_by_type(deed_type.value, limit)
    return {
        "deed_type": deed_type.value,
        "count": len(results),
        "deeds": results,
    }


@router.get("/by-boundary/{name}", summary="Search deeds by boundary reference")
async def get_deeds_by_boundary(
    name: str = Path(..., description="Name referenced in a boundary description"),
):
    """Search deeds by boundary reference (adjacent property owner name)."""
    results = graph_service.search_deeds_by_boundary(name)
    return {
        "boundary_reference": name,
        "count": len(results),
        "deeds": results,
    }


# =============================================================================
# PARAMETERISED ROUTES  —  /{deed_code} wildcard MUST come last
# =============================================================================

@router.get("/{deed_code}", response_model=DeedResponse, summary="Get full deed details")
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
        raise HTTPException(status_code=404, detail=f"Deed '{deed_code}' not found")

    boundaries = None
    if any([deed.get("north"), deed.get("south"), deed.get("east"), deed.get("west")]):
        boundaries = Boundaries(
            north=deed.get("north"),
            south=deed.get("south"),
            east=deed.get("east"),
            west=deed.get("west"),
        )

    property_info = Property(
        lot=deed.get("lot"),
        extent=deed.get("extent"),
        assessment_no=deed.get("assessment"),
        plan_no=deed.get("plan_no"),
        plan_date=deed.get("plan_date"),
        boundaries=boundaries,
    )

    parties = []
    for p in deed.get("parties", []):
        if isinstance(p, dict) and p.get("name"):
            parties.append(Party(
                name=p.get("name", "Unknown"),
                role=p.get("role", "Unknown"),
            ))

    return DeedResponse(
        deed_code=deed.get("deed_code", deed_code),
        deed_type=deed.get("deed_type", "unknown"),
        date=deed.get("date"),
        district=deed.get("district"),
        province=deed.get("province"),
        registry=deed.get("registry"),
        amount=deed.get("amount"),
        property=property_info,
        parties=parties,
        prior_deed=deed.get("prior_deed"),
        governing_statutes=deed.get("governing_statutes", []),
    )


@router.get("/{deed_code}/parties", summary="Get deed parties")
async def get_deed_parties(
    deed_code: str = Path(..., description="Deed code")
):
    """
    Get all parties involved in a deed.

    Returns vendors, vendees, donors, donees, notaries, etc.
    """
    parties = graph_service.get_deed_parties(deed_code)

    if not parties:
        raise HTTPException(
            status_code=404,
            detail=f"No parties found for deed '{deed_code}'"
        )

    return {
        "deed_code": deed_code,
        "parties": [
            {
                "name": p.get("person_name"),
                "role": p.get("role"),
                "nic":  p.get("nic"),
            }
            for p in parties
        ],
    }


@router.get("/{deed_code}/boundaries", summary="Get property boundaries")
async def get_deed_boundaries(
    deed_code: str = Path(..., description="Deed code")
):
    """
    Get property boundaries for a deed (North / South / East / West).
    """
    boundary_data = graph_service.get_deed_boundaries(deed_code)

    if not boundary_data:
        raise HTTPException(
            status_code=404,
            detail=f"No boundary data found for deed '{deed_code}'"
        )

    return {
        "deed_code": boundary_data.get("deed_code", deed_code),
        "lot":    boundary_data.get("lot"),
        "extent": boundary_data.get("extent"),
        "boundaries": {
            "north": boundary_data.get("north"),
            "south": boundary_data.get("south"),
            "east":  boundary_data.get("east"),
            "west":  boundary_data.get("west"),
        },
    }


@router.get("/{deed_code}/history", summary="Get ownership chain")
async def get_ownership_history(
    deed_code: str = Path(..., description="Deed code")
):
    """
    Get ownership chain / history for a deed.

    Traces prior deed references to show how ownership transferred over time.
    """
    chain = graph_service.get_ownership_chain(deed_code)

    if not chain:
        raise HTTPException(
            status_code=404,
            detail=f"No ownership history found for deed '{deed_code}'"
        )

    return {
        "deed_code": deed_code,
        "chain": [
            {
                "deed":            c.get("current_deed"),
                "type":            c.get("deed_type"),
                "date":            c.get("date"),
                "prior_reference": c.get("prior_reference"),
                "prior_deed":      c.get("prior_deed_code"),
                "parties":         c.get("current_parties", []),
            }
            for c in chain
        ],
    }


@router.get("/{deed_code}/governing-law", summary="Get governing statutes")
async def get_governing_law(
    deed_code: str = Path(..., description="Deed code")
):
    """
    Get governing statutes and legal requirements for a deed.
    """
    law_data = graph_service.get_governing_law(deed_code)

    if not law_data:
        raise HTTPException(
            status_code=404,
            detail=f"No governing law found for deed '{deed_code}'"
        )

    return {
        "deed_code":          law_data.get("deed_code", deed_code),
        "deed_type":          law_data.get("deed_type"),
        "governing_statutes": law_data.get("governing_statutes", []),
        "requirements":       law_data.get("requirements", []),
        "stamp_duty":         law_data.get("stamp_duty"),
        "critical_sections":  law_data.get("critical_sections", []),
    }