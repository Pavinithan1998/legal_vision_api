"""
Compliance Router
Handles deed compliance checking and validation
"""

from fastapi import APIRouter, HTTPException, Path
from typing import List

from app.models.schemas import (
    ComplianceCheckRequest, ComplianceResponse, 
    ComplianceItem, DeedType
)
from app.services.graph_service import graph_service
from app.services.llm_service import llm_service
from app.utils.intent_detection import QueryType


router = APIRouter()


@router.post("/check", response_model=ComplianceResponse)
async def check_deed_compliance(request: ComplianceCheckRequest):
    """
    Check if a deed complies with Sri Lankan property law requirements.
    
    Analyzes:
    - Required documentation
    - Notarization
    - Witness requirements
    - Party identification
    - Property description
    - Registration status
    - Stamp duty
    
    Returns compliance score and specific recommendations.
    """
    analysis = graph_service.analyze_compliance(request.deed_code)
    
    if not analysis.get('found'):
        raise HTTPException(
            status_code=404,
            detail=f"Deed {request.deed_code} not found in the system"
        )
    
    # Convert items to proper format
    items = [
        ComplianceItem(
            requirement=item['requirement'],
            status=item['status'],
            details=item.get('details')
        )
        for item in analysis.get('items', [])
    ]
    
    return ComplianceResponse(
        deed_code=analysis['deed_code'],
        deed_type=analysis['deed_type'],
        is_compliant=analysis['is_compliant'],
        compliance_score=analysis['compliance_score'],
        items=items,
        governing_statutes=analysis.get('governing_statutes', []),
        recommendations=analysis.get('recommendations', [])
    )


@router.get("/check/{deed_code}")
async def check_compliance_by_code(
    deed_code: str = Path(..., description="Deed code to check")
):
    """
    Check compliance for a deed by its code.
    
    GET version of the compliance check endpoint.
    """
    analysis = graph_service.analyze_compliance(deed_code)
    
    if not analysis.get('found'):
        raise HTTPException(
            status_code=404,
            detail=f"Deed {deed_code} not found in the system"
        )
    
    return analysis


@router.get("/requirements/{deed_type}")
async def get_compliance_requirements(
    deed_type: DeedType = Path(..., description="Type of deed")
):
    """
    Get all compliance requirements for a specific deed type.
    
    Returns:
    - Mandatory requirements
    - Optional requirements
    - Governing statutes
    - Stamp duty information
    - Registration requirements
    """
    requirements = graph_service.get_deed_requirements(deed_type.value)
    
    if not requirements:
        raise HTTPException(
            status_code=404,
            detail=f"No requirements found for deed type: {deed_type.value}"
        )
    
    # Get governing statutes
    statutes = graph_service.get_statutes_for_deed_type(deed_type.value)
    
    return {
        "deed_type": deed_type.value,
        "requirements": requirements,
        "governing_statutes": [
            {
                "name": s.get('statute_name'),
                "short_name": s.get('short_name'),
                "key_provisions": s.get('key_provisions', [])
            }
            for s in statutes
        ],
        "critical_sections": [
            sec for s in statutes 
            for sec in s.get('critical_sections', [])
        ]
    }


@router.post("/analyze/{deed_code}")
async def analyze_deed_compliance_detailed(
    deed_code: str = Path(..., description="Deed code to analyze")
):
    """
    Perform detailed compliance analysis with legal reasoning.
    
    Uses AI to provide:
    - Detailed compliance assessment
    - IRAC-formatted legal analysis
    - Specific statutory references
    - Remediation recommendations
    """
    # Get deed data
    deed = graph_service.get_deed_by_code(deed_code)
    
    if not deed:
        raise HTTPException(
            status_code=404,
            detail=f"Deed {deed_code} not found"
        )
    
    # Get compliance data
    compliance = graph_service.analyze_compliance(deed_code)
    
    # Get governing law
    law_data = graph_service.get_governing_law(deed_code)
    
    # Get requirements
    deed_type = deed.get('deed_type', 'sale_transfer')
    requirements = graph_service.get_deed_requirements(deed_type)
    
    # Combine all data
    graph_data = [deed]
    if law_data:
        graph_data.append(law_data)
    graph_data.extend(requirements)
    
    # Generate detailed analysis with LLM
    question = f"""Analyze the compliance of deed {deed_code} with Sri Lankan property law.
    
    The deed is a {deed_type}.
    Current compliance score: {compliance.get('compliance_score', 0) * 100}%
    
    Items checked:
    {compliance.get('items', [])}
    
    Provide a detailed legal analysis including:
    1. What requirements are met
    2. What requirements are missing or incomplete
    3. Which statutes apply
    4. Recommendations for remediation
    """
    
    llm_response = llm_service.generate_response(
        user_query=question,
        graph_data=graph_data,
        intent="compliance_analysis",
        query_type=QueryType.COMPLIANCE,
        include_reasoning=True
    )
    
    return {
        "deed_code": deed_code,
        "deed_type": deed_type,
        "basic_compliance": compliance,
        "detailed_analysis": llm_response["answer"],
        "irac_analysis": llm_response.get("irac_analysis"),
        "reasoning_steps": llm_response.get("reasoning_steps"),
        "referenced_statutes": llm_response.get("related_statutes", []),
        "confidence": llm_response.get("confidence", 0.5)
    }


@router.get("/validate/{deed_type}")
async def get_validation_checklist(
    deed_type: DeedType = Path(..., description="Type of deed")
):
    """
    Get a validation checklist for a deed type.
    
    Returns a comprehensive checklist that can be used to validate
    a deed before submission/registration.
    """
    # Get requirements
    requirements = graph_service.get_deed_requirements(deed_type.value)
    
    # Get statutes
    statutes = graph_service.get_statutes_for_deed_type(deed_type.value)
    
    # Build checklist
    checklist = []
    
    if requirements:
        req_data = requirements[0]
        for item in req_data.get('requirements', []):
            checklist.append({
                "item": item,
                "category": "documentation",
                "mandatory": True,
                "statute": "Prevention of Frauds Ordinance"
            })
    
    # Add standard items
    standard_items = [
        {"item": "Parties identified with valid NIC", "category": "parties", "mandatory": True},
        {"item": "Property clearly described with boundaries", "category": "property", "mandatory": True},
        {"item": "Consideration amount stated (if applicable)", "category": "financial", "mandatory": deed_type.value not in ["gift", "will"]},
        {"item": "Prior deed reference included", "category": "chain_of_title", "mandatory": False},
        {"item": "Survey plan attached or referenced", "category": "property", "mandatory": False},
        {"item": "Stamp duty paid", "category": "financial", "mandatory": True},
        {"item": "Registered within 3 months", "category": "registration", "mandatory": True},
    ]
    
    for item in standard_items:
        if not any(c['item'] == item['item'] for c in checklist):
            checklist.append(item)
    
    return {
        "deed_type": deed_type.value,
        "checklist": checklist,
        "governing_statutes": [s.get('statute_name') for s in statutes],
        "notes": [
            "All notarial documents must be executed before a licensed notary public",
            "Two witnesses are required for attestation",
            "Notary must read and explain the document to parties",
            "Registration must be done at the Land Registry of the district where the property is located"
        ]
    }
