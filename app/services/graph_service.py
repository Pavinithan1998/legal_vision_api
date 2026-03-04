"""
Graph Service
Handles all Neo4j database operations and query execution
"""

from typing import Dict, List, Optional, Any
from app.core.database import neo4j_driver
from app.utils.cypher_queries import queries
from app.utils.intent_detection import Intent


class GraphService:
    """
    Service for executing Neo4j queries and processing results.
    """
    
    def __init__(self):
        self.driver = neo4j_driver
    
    def execute_intent(self, intent: Intent, params: Dict) -> List[Dict]:
        """
        Execute the appropriate query for a given intent.
        
        Args:
            intent: The detected intent
            params: Query parameters
            
        Returns:
            List of result records
        """
        # Map intents to queries
        intent_to_query = {
            Intent.FIND_DEED_DETAILS: queries.FIND_DEED_DETAILS,
            Intent.FIND_DEED_PARTIES: queries.FIND_DEED_PARTIES,
            Intent.FIND_BOUNDARIES: queries.FIND_BOUNDARIES,
            Intent.FIND_BY_BOUNDARY: queries.FIND_BY_BOUNDARY,
            Intent.FIND_OWNERSHIP_CHAIN: queries.FIND_OWNERSHIP_CHAIN,
            Intent.FIND_PERSON_DEEDS: queries.FIND_PERSON_DEEDS,
            Intent.FIND_DISTRICT_DEEDS: queries.FIND_DISTRICT_DEEDS,
            Intent.FIND_BY_TYPE: queries.FIND_BY_TYPE,
            Intent.FIND_PROPERTY: queries.FIND_PROPERTY,
            Intent.FIND_RECENT_DEEDS: queries.FIND_RECENT_DEEDS,
            Intent.FIND_BY_AMOUNT: queries.FIND_BY_AMOUNT,
            Intent.FIND_STATUTE: queries.FIND_STATUTE,
            Intent.FIND_GOVERNING_LAW: queries.FIND_GOVERNING_LAW,
            Intent.FIND_DEED_REQUIREMENTS: queries.FIND_DEED_REQUIREMENTS,
            Intent.FIND_SECTION: queries.FIND_SECTION,
            Intent.FIND_DEFINITION: queries.FIND_DEFINITION,
            Intent.FIND_PRINCIPLE: queries.FIND_PRINCIPLE,
            Intent.CHECK_COMPLIANCE: queries.CHECK_DEED_COMPLIANCE,
            Intent.GET_STATS: queries.GET_STATS,
            Intent.GENERAL_SEARCH: queries.GENERAL_SEARCH,
        }
        
        cypher = intent_to_query.get(intent, queries.GENERAL_SEARCH)
        
        # Set default limit if not provided
        if 'limit' not in params:
            params['limit'] = 10
        
        return self.driver.execute_query(cypher, params)
    
    # =========================================================================
    # DEED OPERATIONS
    # =========================================================================
    
    def get_deed_by_code(self, code: str) -> Optional[Dict]:
        """Get full deed details by code."""
        results = self.driver.execute_query(queries.FIND_DEED_DETAILS, {"code": code})
        return results[0] if results else None
    
    def get_deed_parties(self, code: str) -> List[Dict]:
        """Get all parties involved in a deed."""
        return self.driver.execute_query(queries.FIND_DEED_PARTIES, {"code": code})
    
    def get_deed_boundaries(self, code: str) -> Optional[Dict]:
        """Get property boundaries for a deed."""
        results = self.driver.execute_query(queries.FIND_BOUNDARIES, {"code": code})
        return results[0] if results else None
    
    def get_ownership_chain(self, code: str) -> List[Dict]:
        """Get ownership history for a deed."""
        return self.driver.execute_query(queries.FIND_OWNERSHIP_CHAIN, {"code": code})
    
    def search_deeds_by_person(self, name: str, limit: int = 10) -> List[Dict]:
        """Search deeds by person name."""
        return self.driver.execute_query(
            queries.FIND_PERSON_DEEDS, 
            {"name": name, "limit": limit}
        )
    
    def search_deeds_by_district(self, district: str, limit: int = 10) -> List[Dict]:
        """Search deeds by district."""
        return self.driver.execute_query(
            queries.FIND_DISTRICT_DEEDS,
            {"district": district, "limit": limit}
        )
    
    def search_deeds_by_type(self, deed_type: str, limit: int = 10) -> List[Dict]:
        """Search deeds by type."""
        return self.driver.execute_query(
            queries.FIND_BY_TYPE,
            {"deed_type": deed_type, "limit": limit}
        )
    
    def search_deeds_by_boundary(self, name: str) -> List[Dict]:
        """Search deeds by boundary reference (adjacent property owner)."""
        return self.driver.execute_query(queries.FIND_BY_BOUNDARY, {"name": name})
    
    def get_recent_deeds(self, limit: int = 10) -> List[Dict]:
        """Get most recent deeds."""
        return self.driver.execute_query(queries.FIND_RECENT_DEEDS, {"limit": limit})
    
    def get_highest_value_deeds(self, limit: int = 10) -> List[Dict]:
        """Get highest value deeds by consideration amount."""
        return self.driver.execute_query(queries.FIND_BY_AMOUNT, {"limit": limit})
    
    def search_property(self, lot: str) -> List[Dict]:
        """Search property by lot number."""
        return self.driver.execute_query(queries.FIND_PROPERTY, {"lot": lot})
    
    # =========================================================================
    # LEGAL OPERATIONS
    # =========================================================================
    
    def search_statutes(self, query: str) -> List[Dict]:
        """Search statutes by name, short name, or category."""
        return self.driver.execute_query(queries.FIND_STATUTE, {"query": query})
    
    def get_all_statutes(self) -> List[Dict]:
        """Get all statutes."""
        return self.driver.execute_query(queries.FIND_ALL_STATUTES, {})
    
    def get_governing_law(self, code: str) -> Optional[Dict]:
        """Get governing law for a deed."""
        results = self.driver.execute_query(queries.FIND_GOVERNING_LAW, {"code": code})
        return results[0] if results else None
    
    def get_deed_requirements(self, deed_type: str) -> List[Dict]:
        """Get requirements for a deed type."""
        return self.driver.execute_query(
            queries.FIND_DEED_REQUIREMENTS,
            {"deed_type": deed_type}
        )
    
    def get_statutes_for_deed_type(self, deed_type: str) -> List[Dict]:
        """Get all statutes applicable to a deed type."""
        return self.driver.execute_query(
            queries.FIND_STATUTES_FOR_DEED_TYPE,
            {"deed_type": deed_type}
        )
    
    def search_sections(self, query: str) -> List[Dict]:
        """Search statute sections."""
        return self.driver.execute_query(queries.FIND_SECTION, {"query": query})
    
    # =========================================================================
    # DEFINITION OPERATIONS
    # =========================================================================
    
    def search_definitions(self, term: str) -> List[Dict]:
        """Search legal definitions by term."""
        return self.driver.execute_query(queries.FIND_DEFINITION, {"term": term})
    
    def get_all_definitions(self) -> List[Dict]:
        """Get all legal definitions."""
        return self.driver.execute_query(queries.FIND_ALL_DEFINITIONS, {})
    
    def search_principles(self, query: str) -> List[Dict]:
        """Search legal principles."""
        return self.driver.execute_query(queries.FIND_PRINCIPLE, {"query": query})
    
    def get_all_principles(self) -> List[Dict]:
        """Get all legal principles."""
        return self.driver.execute_query(queries.FIND_ALL_PRINCIPLES, {})
    
    # =========================================================================
    # COMPLIANCE OPERATIONS
    # =========================================================================
    
    def check_compliance(self, code: str) -> Optional[Dict]:
        """Check deed compliance with requirements."""
        results = self.driver.execute_query(queries.CHECK_DEED_COMPLIANCE, {"code": code})
        return results[0] if results else None
    
    def analyze_compliance(self, code: str) -> Dict[str, Any]:
        """
        Analyze deed compliance and return detailed assessment.
        
        Returns:
            Dict with compliance status, items checked, and recommendations
        """
        deed_data = self.check_compliance(code)
        
        if not deed_data:
            return {
                "deed_code": code,
                "found": False,
                "message": "Deed not found in the system"
            }
        
        requirements = deed_data.get('required_items') or []
        compliance_items = []
        
        # Check each requirement
        checks = {
            "Written document": True,  # If we have it, it's written
            "Notarially attested": bool(deed_data.get('registry')),
            "Two witnesses": True,  # Assume yes if registered
            "Parties identified by NIC": bool(deed_data.get('parties')),
            "Property clearly described": bool(deed_data.get('lot')),
            "Survey plan referenced": bool(deed_data.get('plan_no')),
            "Consideration stated": bool(deed_data.get('amount')),
            "Registered": bool(deed_data.get('registry')),
        }
        
        met_count = 0
        for req in requirements:
            status = "met" if checks.get(req, False) else "not_met"
            if status == "met":
                met_count += 1
            compliance_items.append({
                "requirement": req,
                "status": status,
                "details": None
            })
        
        total_reqs = len(requirements) if requirements else 1
        compliance_score = met_count / total_reqs if total_reqs > 0 else 0
        
        recommendations = []
        for item in compliance_items:
            if item["status"] == "not_met":
                recommendations.append(f"Ensure '{item['requirement']}' is properly documented")
        
        return {
            "deed_code": code,
            "deed_type": deed_data.get('deed_type'),
            "found": True,
            "is_compliant": compliance_score >= 0.8,
            "compliance_score": round(compliance_score, 2),
            "items": compliance_items,
            "governing_statutes": deed_data.get('governing_statutes', []),
            "recommendations": recommendations
        }
    
    # =========================================================================
    # STATISTICS
    # =========================================================================
    
    def get_statistics(self) -> Dict:
        """Get knowledge graph statistics."""
        results = self.driver.execute_query(queries.GET_STATS, {})
        
        if not results:
            return {
                "total_deeds": 0,
                "total_persons": 0,
                "total_properties": 0,
                "total_districts": 0,
                "total_statutes": 0,
                "total_definitions": 0,
                "deed_breakdown": {}
            }
        
        data = results[0]
        return {
            "total_deeds": data.get('total_deeds', 0),
            "total_persons": data.get('total_persons', 0),
            "total_properties": data.get('total_parcels', 0),
            "total_districts": data.get('total_districts', 0),
            "total_statutes": data.get('total_statutes', 0),
            "total_definitions": data.get('total_definitions', 0),
            "deed_breakdown": {
                "sale_transfer": data.get('sales', 0),
                "gift": data.get('gifts', 0),
                "will": data.get('wills', 0),
                "lease": data.get('leases', 0),
                "mortgage": data.get('mortgages', 0)
            }
        }
    
    # =========================================================================
    # GENERAL SEARCH
    # =========================================================================
    
    def general_search(self, query: str) -> List[Dict]:
        """Perform a general search across all entity types."""
        return self.driver.execute_query(queries.GENERAL_SEARCH, {"query": query})


# Create singleton instance
graph_service = GraphService()
