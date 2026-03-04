"""
Pydantic Models for API Requests and Responses
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


# =============================================================================
# ENUMS
# =============================================================================

class DeedType(str, Enum):
    SALE_TRANSFER = "sale_transfer"
    GIFT = "gift"
    WILL = "will"
    LEASE = "lease"
    MORTGAGE = "mortgage"
    PARTITION = "partition"
    UNKNOWN = "unknown"


class QueryType(str, Enum):
    FACTUAL = "factual"
    LEGAL_REASONING = "legal_reasoning"
    DEFINITION = "definition"
    COMPLIANCE = "compliance"
    COMPARISON = "comparison"


# =============================================================================
# REQUEST MODELS
# =============================================================================

class QueryRequest(BaseModel):
    """Natural language query request."""
    query: str = Field(..., description="Natural language question", min_length=3)
    session_id: Optional[str] = Field(None, description="Session ID for conversation context")
    include_reasoning: bool = Field(True, description="Include legal reasoning in response")
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "What laws govern sale deeds in Sri Lanka?",
                "session_id": "user-123",
                "include_reasoning": True
            }
        }


class DeedSearchRequest(BaseModel):
    """Deed search request."""
    code: Optional[str] = Field(None, description="Deed code (e.g., 'A 1100/188')")
    deed_type: Optional[DeedType] = Field(None, description="Type of deed")
    district: Optional[str] = Field(None, description="District name")
    person_name: Optional[str] = Field(None, description="Party name to search")
    limit: int = Field(10, ge=1, le=50, description="Maximum results")


class ComplianceCheckRequest(BaseModel):
    """Deed compliance check request."""
    deed_code: str = Field(..., description="Deed code to check")
    
    class Config:
        json_schema_extra = {
            "example": {
                "deed_code": "A 1100/188"
            }
        }


class LegalQueryRequest(BaseModel):
    """Legal reasoning query request."""
    question: str = Field(..., description="Legal question")
    deed_code: Optional[str] = Field(None, description="Related deed code")
    deed_type: Optional[DeedType] = Field(None, description="Related deed type")
    include_irac: bool = Field(True, description="Include IRAC analysis")


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class Party(BaseModel):
    """Party involved in a deed."""
    name: str
    role: str
    nic: Optional[str] = None


class Boundaries(BaseModel):
    """Property boundaries."""
    north: Optional[str] = None
    south: Optional[str] = None
    east: Optional[str] = None
    west: Optional[str] = None


class Property(BaseModel):
    """Property parcel information."""
    lot: Optional[str] = None
    extent: Optional[str] = None
    assessment_no: Optional[str] = None
    plan_no: Optional[str] = None
    plan_date: Optional[str] = None
    boundaries: Optional[Boundaries] = None


class DeedResponse(BaseModel):
    """Full deed information response."""
    deed_code: str
    deed_type: str
    date: Optional[str] = None
    district: Optional[str] = None
    province: Optional[str] = None
    registry: Optional[str] = None
    amount: Optional[float] = None
    property: Optional[Property] = None
    parties: List[Party] = []
    prior_deed: Optional[str] = None
    governing_statutes: List[str] = []


class StatuteResponse(BaseModel):
    """Statute information response."""
    name: str
    short_name: Optional[str] = None
    act_number: Optional[str] = None
    year: Optional[int] = None
    category: Optional[str] = None
    description: Optional[str] = None
    key_provisions: List[str] = []
    applies_to: List[str] = []


class SectionResponse(BaseModel):
    """Statute section response."""
    section_number: str
    title: str
    content: str
    importance: Optional[str] = None
    statute_name: str


class DefinitionResponse(BaseModel):
    """Legal definition response."""
    term: str
    definition: str
    source: Optional[str] = None


class PrincipleResponse(BaseModel):
    """Legal principle response."""
    name: str
    english_meaning: str
    description: str
    application: Optional[str] = None


class IRACAnalysis(BaseModel):
    """IRAC legal analysis."""
    issue: str
    rule: str
    application: str
    conclusion: str


class ReasoningStep(BaseModel):
    """Single reasoning step."""
    step: int
    text: str
    legal_basis: Optional[str] = None


class ComplianceItem(BaseModel):
    """Single compliance check item."""
    requirement: str
    status: str  # "met", "not_met", "unknown"
    details: Optional[str] = None


class ComplianceResponse(BaseModel):
    """Deed compliance check response."""
    deed_code: str
    deed_type: str
    is_compliant: bool
    compliance_score: float
    items: List[ComplianceItem]
    governing_statutes: List[str]
    recommendations: List[str] = []


class QueryResponse(BaseModel):
    """Natural language query response."""
    query: str
    intent: str
    query_type: str
    answer: str
    reasoning_steps: Optional[List[ReasoningStep]] = None
    irac_analysis: Optional[IRACAnalysis] = None
    sources: List[str] = []
    related_statutes: List[str] = []
    confidence: float = Field(ge=0, le=1)
    data: Optional[Dict[str, Any]] = None


class StatsResponse(BaseModel):
    """Knowledge graph statistics."""
    total_deeds: int
    total_persons: int
    total_properties: int
    total_districts: int
    total_statutes: int
    total_definitions: int
    deed_breakdown: Dict[str, int]


class ErrorResponse(BaseModel):
    """Error response."""
    error: str
    detail: Optional[str] = None
    code: Optional[str] = None
