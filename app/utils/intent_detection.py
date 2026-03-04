"""
Intent Detection and NLP Utilities
Handles query parsing, intent classification, and entity extraction
"""

import re
from typing import Tuple, Dict, Optional, List
from enum import Enum


class Intent(str, Enum):
    """Query intent types."""
    # Deed intents
    FIND_DEED_DETAILS = "find_deed_details"
    FIND_DEED_PARTIES = "find_deed_parties"
    FIND_BOUNDARIES = "find_boundaries"
    FIND_BY_BOUNDARY = "find_by_boundary"
    FIND_OWNERSHIP_CHAIN = "find_ownership_chain"
    FIND_PERSON_DEEDS = "find_person_deeds"
    FIND_DISTRICT_DEEDS = "find_district_deeds"
    FIND_BY_TYPE = "find_by_type"
    FIND_PROPERTY = "find_property"
    FIND_RECENT_DEEDS = "find_recent_deeds"
    FIND_BY_AMOUNT = "find_by_amount"
    
    # Legal intents
    FIND_STATUTE = "find_statute"
    FIND_GOVERNING_LAW = "find_governing_law"
    FIND_DEED_REQUIREMENTS = "find_deed_requirements"
    FIND_SECTION = "find_section"
    
    # Definition intents
    FIND_DEFINITION = "find_definition"
    FIND_PRINCIPLE = "find_principle"
    
    # Compliance intents
    CHECK_COMPLIANCE = "check_compliance"
    
    # Statistics
    GET_STATS = "get_stats"
    
    # General
    GENERAL_SEARCH = "general_search"


class QueryType(str, Enum):
    """Query classification types."""
    FACTUAL = "factual"
    LEGAL_REASONING = "legal_reasoning"
    DEFINITION = "definition"
    COMPLIANCE = "compliance"
    COMPARISON = "comparison"


class IntentDetector:
    """
    Detects intent and extracts entities from natural language queries.
    """
    
    def __init__(self):
        # Synonyms for normalization
        self.synonyms = {
            # Party terms
            "seller": "vendor", "buyer": "vendee", "purchaser": "vendee",
            "giver": "donor", "receiver": "donee", "recipient": "donee",
            "owner": "vendor", "notary public": "notary",
            # Property terms
            "land": "property", "plot": "lot", "parcel": "property",
            "ground": "property", "real estate": "property",
            # Deed terms
            "transfer": "sale_transfer", "sale": "sale_transfer",
            "conveyance": "sale_transfer", "donation": "gift",
            # Boundary terms
            "adjacent": "boundary", "neighbor": "boundary", "neighbouring": "boundary",
            "next to": "boundary", "beside": "boundary", "bordering": "boundary",
            # Legal terms
            "law": "statute", "act": "statute", "ordinance": "statute",
            "rule": "statute", "regulation": "statute",
        }
        
        # Sri Lankan districts
        self.districts = [
            'colombo', 'gampaha', 'kalutara', 'kandy', 'matale', 'nuwara eliya',
            'galle', 'matara', 'hambantota', 'jaffna', 'kilinochchi', 'mannar',
            'mullaitivu', 'vavuniya', 'batticaloa', 'ampara', 'trincomalee',
            'kurunegala', 'puttalam', 'anuradhapura', 'polonnaruwa',
            'badulla', 'monaragala', 'ratnapura', 'kegalle'
        ]
        
        # Deed types
        self.deed_types = {
            'sale': 'sale_transfer', 'transfer': 'sale_transfer', 'sale_transfer': 'sale_transfer',
            'gift': 'gift', 'donation': 'gift',
            'will': 'will', 'testament': 'will',
            'lease': 'lease', 'rent': 'lease', 'rental': 'lease',
            'mortgage': 'mortgage', 'loan': 'mortgage',
            'partition': 'partition'
        }
    
    def normalize_query(self, query: str) -> str:
        """Normalize query by replacing synonyms."""
        result = query.lower()
        for synonym, standard in self.synonyms.items():
            result = result.replace(synonym, standard)
        return result
    
    def extract_deed_code(self, query: str) -> Optional[str]:
        """Extract deed code from query."""
        patterns = [
            r'([A-Z]\s*\d+/\d+)',   # A 1100/188
            r'([A-Z]\s*\d+-\d+)',    # A 1100-188
            r'(\d+/\d+)',            # 1100/188
            r'([A-Z]\s*\d{3,})',     # A 1100
            r'DEED[_\s]*(\d+)',      # DEED_001
        ]
        for pattern in patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return None
    
    def extract_person_name(self, query: str) -> Optional[str]:
        """Extract person name from query."""
        # UPPERCASE NAMES
        match = re.search(r'\b([A-Z]{2,}(?:\s+[A-Z]{2,})+)\b', query)
        if match:
            return match.group(1)
        
        # Title Case Names
        match = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b', query)
        if match:
            return match.group(1)
        
        # After keywords
        for keyword in ['named', 'called', 'name', 'person', 'by', 'of', 'involving', 'between']:
            if keyword in query.lower():
                idx = query.lower().find(keyword) + len(keyword)
                remaining = query[idx:].strip()
                words = remaining.split()[:3]
                name_words = [w for w in words if w.lower() not in 
                             ['the', 'a', 'an', 'is', 'was', 'are', 'property', 'deed', 'land', 'and']]
                if name_words:
                    return " ".join(name_words).strip('.,;:?')
        
        return None
    
    def extract_district(self, query: str) -> Optional[str]:
        """Extract district name from query."""
        query_lower = query.lower()
        for district in self.districts:
            if district in query_lower:
                return district.title()
        
        # Check for "district of X" pattern
        match = re.search(r'district\s+(?:of\s+)?(\w+)', query_lower)
        if match:
            return match.group(1).title()
        
        return None
    
    def extract_deed_type(self, query: str) -> Optional[str]:
        """Extract deed type from query."""
        query_lower = query.lower()
        for keyword, deed_type in self.deed_types.items():
            if keyword in query_lower:
                return deed_type
        return None
    
    def extract_legal_term(self, query: str) -> Optional[str]:
        """Extract legal term being asked about."""
        query_lower = query.lower()
        
        # "What is X" pattern
        match = re.search(r'what\s+is\s+(?:a\s+|an\s+)?([a-z\s]+?)(?:\?|$|in\s)', query_lower)
        if match:
            return match.group(1).strip()
        
        # "Define X" pattern
        match = re.search(r'define\s+(?:the\s+)?([a-z\s]+?)(?:\?|$)', query_lower)
        if match:
            return match.group(1).strip()
        
        # "Meaning of X" pattern
        match = re.search(r'meaning\s+of\s+(?:the\s+)?([a-z\s]+?)(?:\?|$)', query_lower)
        if match:
            return match.group(1).strip()
        
        return None
    
    def classify_query_type(self, query: str) -> QueryType:
        """Classify query type for response formatting."""
        query_lower = query.lower()
        
        # Compliance queries
        if any(word in query_lower for word in ['valid', 'comply', 'compliant', 'requirement', 'needed', 'missing', 'check']):
            return QueryType.COMPLIANCE
        
        # Definition queries
        if any(phrase in query_lower for phrase in ['what is', 'what are', 'define', 'meaning of', 'explain']):
            if not self.extract_deed_code(query):
                return QueryType.DEFINITION
        
        # Legal reasoning queries
        if any(word in query_lower for word in ['why', 'legal', 'law', 'statute', 'can i', 'should i', 'is it legal', 'allowed']):
            return QueryType.LEGAL_REASONING
        
        # Comparison queries
        if any(word in query_lower for word in ['difference', 'compare', 'versus', 'vs', 'between']):
            return QueryType.COMPARISON
        
        return QueryType.FACTUAL
    
    def detect_intent(self, query: str, context: Dict = None) -> Tuple[Intent, Dict, QueryType]:
        """
        Detect intent, extract parameters, and classify query type.
        
        Args:
            query: Natural language query
            context: Conversation context for follow-up queries
            
        Returns:
            Tuple of (intent, params, query_type)
        """
        query_lower = query.lower()
        normalized = self.normalize_query(query)
        query_type = self.classify_query_type(query)
        context = context or {}
        
        # =================================================================
        # STEP 1: Handle follow-up questions using context
        # =================================================================
        reference_words = ['this', 'that', 'it', 'its', 'the same', 'these', 'those', 'their']
        has_reference = any(word in query_lower for word in reference_words)
        
        if has_reference and context.get("deed_code"):
            # Boundaries
            if any(word in query_lower for word in ['boundary', 'boundaries', 'border', 'adjacent', 'neighbor']):
                return Intent.FIND_BOUNDARIES, {"code": context["deed_code"]}, query_type
            
            # Parties
            if any(word in query_lower for word in ['party', 'parties', 'owner', 'vendor', 'vendee', 'who', 'involved']):
                return Intent.FIND_DEED_PARTIES, {"code": context["deed_code"]}, query_type
            
            # Details
            if any(word in query_lower for word in ['detail', 'more', 'about', 'tell', 'show', 'describe']):
                return Intent.FIND_DEED_DETAILS, {"code": context["deed_code"]}, query_type
            
            # History
            if any(word in query_lower for word in ['history', 'chain', 'previous', 'prior', 'before']):
                return Intent.FIND_OWNERSHIP_CHAIN, {"code": context["deed_code"]}, query_type
            
            # Governing law
            if any(word in query_lower for word in ['law', 'statute', 'legal', 'govern', 'rule']):
                return Intent.FIND_GOVERNING_LAW, {"code": context["deed_code"]}, query_type
            
            # Compliance
            if any(word in query_lower for word in ['valid', 'comply', 'requirement']):
                return Intent.CHECK_COMPLIANCE, {"code": context["deed_code"]}, query_type
        
        # =================================================================
        # STEP 2: Statistics queries
        # =================================================================
        if any(word in query_lower for word in ['how many', 'count', 'total', 'statistics', 'stats', 'summary', 'overview']):
            return Intent.GET_STATS, {}, query_type
        
        # =================================================================
        # STEP 3: Definition queries
        # =================================================================
        if query_type == QueryType.DEFINITION:
            term = self.extract_legal_term(query)
            if term:
                # Check if it's a legal principle
                if any(word in query_lower for word in ['principle', 'doctrine', 'maxim', 'nemo', 'caveat']):
                    return Intent.FIND_PRINCIPLE, {"query": term}, query_type
                return Intent.FIND_DEFINITION, {"term": term}, query_type
        
        # =================================================================
        # STEP 4: Legal/statute queries
        # =================================================================
        if any(word in query_lower for word in ['statute', 'law', 'ordinance', 'act', 'section', 'provision']):
            # Requirements for a deed type
            deed_type = self.extract_deed_type(query)
            if deed_type and any(word in query_lower for word in ['requirement', 'need', 'must', 'should']):
                return Intent.FIND_DEED_REQUIREMENTS, {"deed_type": deed_type}, query_type
            
            # Governing law for a deed
            code = self.extract_deed_code(query)
            if code:
                return Intent.FIND_GOVERNING_LAW, {"code": code}, query_type
            
            # General statute search
            return Intent.FIND_STATUTE, {"query": query}, query_type
        
        # =================================================================
        # STEP 5: Compliance check
        # =================================================================
        if query_type == QueryType.COMPLIANCE:
            code = self.extract_deed_code(query)
            if code:
                return Intent.CHECK_COMPLIANCE, {"code": code}, query_type
            deed_type = self.extract_deed_type(query)
            if deed_type:
                return Intent.FIND_DEED_REQUIREMENTS, {"deed_type": deed_type}, query_type
        
        # =================================================================
        # STEP 6: Recent/latest queries
        # =================================================================
        if any(word in query_lower for word in ['recent', 'latest', 'newest', 'last']):
            return Intent.FIND_RECENT_DEEDS, {"limit": 10}, query_type
        
        # =================================================================
        # STEP 7: Amount/price queries
        # =================================================================
        if any(word in query_lower for word in ['expensive', 'costly', 'highest price', 'amount', 'price', 'value', 'consideration']):
            return Intent.FIND_BY_AMOUNT, {"limit": 10}, query_type
        
        # =================================================================
        # STEP 8: Deed type queries
        # =================================================================
        deed_type = self.extract_deed_type(query)
        if deed_type and any(w in query_lower for w in ['all', 'list', 'find', 'show', 'type']):
            return Intent.FIND_BY_TYPE, {"deed_type": deed_type, "limit": 10}, query_type
        
        # =================================================================
        # STEP 9: Boundary/neighbor search
        # =================================================================
        if any(word in query_lower for word in ['adjacent', 'neighbor', 'boundary', 'next to', 'beside', 'claimed']):
            name = self.extract_person_name(query)
            if name:
                return Intent.FIND_BY_BOUNDARY, {"name": name}, query_type
        
        # =================================================================
        # STEP 10: Person/party search
        # =================================================================
        if any(word in query_lower for word in ['who', 'person', 'owner', 'party', 'vendor', 'vendee', 'donor', 'donee', 'notary', 'involved']):
            code = self.extract_deed_code(query)
            if code:
                return Intent.FIND_DEED_PARTIES, {"code": code}, query_type
            
            name = self.extract_person_name(query)
            if name:
                return Intent.FIND_PERSON_DEEDS, {"name": name, "limit": 10}, query_type
        
        # =================================================================
        # STEP 11: District search
        # =================================================================
        district = self.extract_district(query)
        if district:
            return Intent.FIND_DISTRICT_DEEDS, {"district": district, "limit": 10}, query_type
        
        # =================================================================
        # STEP 12: Deed code/details search
        # =================================================================
        code = self.extract_deed_code(query)
        if code:
            if any(word in query_lower for word in ['boundary', 'boundaries']):
                return Intent.FIND_BOUNDARIES, {"code": code}, query_type
            if any(word in query_lower for word in ['party', 'parties', 'who']):
                return Intent.FIND_DEED_PARTIES, {"code": code}, query_type
            if any(word in query_lower for word in ['history', 'chain', 'prior']):
                return Intent.FIND_OWNERSHIP_CHAIN, {"code": code}, query_type
            if any(word in query_lower for word in ['law', 'statute', 'govern']):
                return Intent.FIND_GOVERNING_LAW, {"code": code}, query_type
            return Intent.FIND_DEED_DETAILS, {"code": code}, query_type
        
        # =================================================================
        # STEP 13: Property/lot search
        # =================================================================
        if any(word in query_lower for word in ['lot', 'property', 'parcel', 'plan', 'plot']):
            lot_match = re.search(r'lot\s*([0-9A-Za-z]+)', query_lower)
            if lot_match:
                return Intent.FIND_PROPERTY, {"lot": lot_match.group(1).upper()}, query_type
            
            plan_match = re.search(r'plan\s*(?:no\.?)?\s*([0-9]+)', query_lower)
            if plan_match:
                return Intent.FIND_PROPERTY, {"lot": plan_match.group(1)}, query_type
        
        # =================================================================
        # STEP 14: Person name search
        # =================================================================
        name = self.extract_person_name(query)
        if name:
            return Intent.FIND_PERSON_DEEDS, {"name": name, "limit": 10}, query_type
        
        # =================================================================
        # DEFAULT: General search
        # =================================================================
        stop_words = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'what', 'where', 'when', 
                     'how', 'why', 'who', 'which', 'me', 'tell', 'show', 'find', 'about',
                     'can', 'you', 'please', 'i', 'want', 'need', 'to', 'know', 'of', 'in'}
        words = [w for w in query.split() if w.lower() not in stop_words]
        search_query = " ".join(words) if words else query
        
        return Intent.GENERAL_SEARCH, {"query": search_query}, query_type


# Create singleton instance
intent_detector = IntentDetector()
