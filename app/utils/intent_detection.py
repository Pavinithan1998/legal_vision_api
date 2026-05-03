"""
Intent Detection and NLP Utilities
Handles query parsing, intent classification, and entity extraction

UPDATED: Better recognition of cost, price, comparison, and transfer questions
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
    
    # NEW: Cost and price intents
    CALCULATE_TRANSFER_COST = "calculate_transfer_cost"
    COMPARE_DEED_TYPES = "compare_deed_types"
    FIND_LAND_PRICE = "find_land_price"
    FIND_STAMP_DUTY = "find_stamp_duty"
    FIND_REGISTRY = "find_registry"


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
        
        # Known areas for price queries
        self.known_areas = [
            'athurugiriya', 'nugegoda', 'maharagama', 'rajagiriya', 'dehiwala',
            'moratuwa', 'kelaniya', 'negombo', 'ja-ela', 'kadawatha', 'kiribathgoda',
            'kolonnawa', 'homagama', 'kaduwela', 'malabe', 'battaramulla',
            'pelawatte', 'thalawathugoda', 'kotte', 'nawala', 'boralesgamuwa',
            'piliyandala', 'pannipitiya', 'kottawa', 'mount lavinia',
            'peradeniya', 'katugastota', 'digana', 'kundasale',
            'unawatuna', 'hikkaduwa', 'mirissa', 'weligama', 'tangalle',
            'chilaw', 'kuliyapitiya', 'mawanella',
            'bandarawela', 'haputale', 'ella',
            'nallur', 'chavakachcheri'
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
        """
        Extract deed code from query.
        Patterns are ordered from most-specific to least-specific so that
        codes like 'M 26028/77' or 'A 1100/188' match before bare numbers.
        Whitespace inside the code is normalised to a single space.
        """
        patterns = [
            r'\b([A-Za-z]\s*\d{2,}\s*/\s*\d{1,})\b',   # M 26028/77, A 1100/188
            r'\b([A-Za-z]\s*\d{2,}\s*-\s*\d{1,})\b',   # M 26028-77, A 1100-188
            r'\b(\d{2,}\s*/\s*\d{1,})\b',              # 1100/188 (no letter prefix)
            r'\bDEED[_\s-]*(\d+)\b',                   # DEED_001, DEED-001
            r'\b([A-Za-z]\s*\d{3,})\b',                # A 1100 (fallback, letter + 3+ digits)
        ]
        for pattern in patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                # Normalise: collapse internal whitespace and uppercase the leading letter
                code = re.sub(r'\s+', ' ', match.group(1).strip())
                # Standardise letter prefix to uppercase
                if code and code[0].isalpha():
                    code = code[0].upper() + code[1:]
                return code
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
    
    def extract_area_name(self, query: str) -> Optional[str]:
        """Extract area/location name from query."""
        query_lower = query.lower()
        
        # Check for known areas first
        for area in self.known_areas:
            if area in query_lower:
                return area.title()
        
        # Check for Colombo zones (Colombo 1-15)
        colombo_match = re.search(r'colombo\s*(\d{1,2})', query_lower)
        if colombo_match:
            return f"Colombo {colombo_match.group(1)}"
        
        # Check for "in [Place]" pattern
        match = re.search(r'\bin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)', query)
        if match:
            place = match.group(1)
            # Filter out common non-place words
            if place.lower() not in ['sri', 'the', 'this', 'that', 'which', 'what']:
                return place
        
        # Fall back to district
        return self.extract_district(query)
    
    def extract_amount(self, query: str) -> Optional[float]:
        """Extract monetary amount from query."""
        query_lower = query.lower()
        
        # Pattern: 10 million, 10M, 10,000,000, Rs. 10000000
        patterns = [
            (r'(\d+(?:\.\d+)?)\s*(?:million|m\b)', 1000000),
            (r'(\d+(?:\.\d+)?)\s*(?:lakh|lakhs|lac)', 100000),
            (r'(\d+(?:\.\d+)?)\s*(?:crore|cr)', 10000000),
            (r'(?:rs\.?|lkr|rupees?)\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', 1),
            (r'(\d{1,3}(?:,\d{3})+)', 1),  # Comma-separated numbers like 10,000,000
        ]
        
        for pattern, multiplier in patterns:
            match = re.search(pattern, query_lower)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    return float(amount_str) * multiplier
                except ValueError:
                    continue
        
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
        
        # Comparison queries (check first)
        if any(word in query_lower for word in ['difference', 'compare', 'versus', 'vs', 'between', 'better', 'which type', 'which deed']):
            return QueryType.COMPARISON
        
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
        # PRIORITY STEP: Cost/Price/Transfer questions (check FIRST)
        # =================================================================
        
        is_cost_question = any(phrase in query_lower for phrase in [
            'cost', 'how much', 'price', 'stamp duty', 'fees', 'charges',
            'pay', 'expense', 'budget', 'calculate', 'total', 'estimate',
            'what will', 'will it cost', 'cost me'
        ])
        
        is_transfer_question = any(phrase in query_lower for phrase in [
            'transfer', 'give to', 'give my', 'to my son', 'to my daughter',
            'to my wife', 'to my husband', 'to my child', 'to my parent',
            'to my brother', 'to my sister', 'family', 'pass on', 'hand over',
            'my land', 'my property'
        ])
        
        is_comparison = any(phrase in query_lower for phrase in [
            'compare', 'vs', 'versus', 'or', 'better', 'difference',
            'which deed', 'what type', 'best deed', 'best way', 'which is'
        ])
        
        is_land_price_question = any(phrase in query_lower for phrase in [
            'land price', 'property price', 'per perch', 'price in',
            'prices in', 'market value', 'market price', 'land value',
            'how much is land', 'land cost'
        ])
        
        # -----------------------------------------------------------------
        # Cost calculation WITH comparison (gift vs sale)
        # -----------------------------------------------------------------
        if is_cost_question and is_comparison:
            deed_type = self.extract_deed_type(query) or "sale_transfer"
            area = self.extract_area_name(query)
            amount = self.extract_amount(query)
            
            return Intent.COMPARE_DEED_TYPES, {
                "deed_type": deed_type,
                "area": area,
                "amount": amount,
                "comparison": True
            }, QueryType.COMPARISON
        
        # -----------------------------------------------------------------
        # Cost calculation for family transfer
        # -----------------------------------------------------------------
        if is_cost_question and is_transfer_question:
            deed_type = self.extract_deed_type(query)
            area = self.extract_area_name(query)
            amount = self.extract_amount(query)
            
            # Default to gift for family transfers (better for user)
            if not deed_type:
                deed_type = "gift"
            
            return Intent.CALCULATE_TRANSFER_COST, {
                "deed_type": deed_type,
                "area": area or "Colombo",
                "amount": amount,
                "is_family_transfer": True
            }, QueryType.FACTUAL
        
        # -----------------------------------------------------------------
        # General cost question
        # -----------------------------------------------------------------
        if is_cost_question:
            deed_type = self.extract_deed_type(query)
            amount = self.extract_amount(query)
            area = self.extract_area_name(query)
            
            return Intent.CALCULATE_TRANSFER_COST, {
                "deed_type": deed_type or "sale_transfer",
                "amount": amount,
                "area": area
            }, QueryType.FACTUAL
        
        # -----------------------------------------------------------------
        # Stamp duty specific questions
        # -----------------------------------------------------------------
        if 'stamp duty' in query_lower:
            deed_type = self.extract_deed_type(query)
            amount = self.extract_amount(query)
            
            return Intent.FIND_STAMP_DUTY, {
                "deed_type": deed_type or "sale_transfer",
                "amount": amount
            }, QueryType.FACTUAL
        
        # -----------------------------------------------------------------
        # Land price questions
        # -----------------------------------------------------------------
        if is_land_price_question:
            area = self.extract_area_name(query)
            
            return Intent.FIND_LAND_PRICE, {
                "area_name": area or "Colombo"
            }, QueryType.FACTUAL
        
        # -----------------------------------------------------------------
        # Deed type comparison (without cost focus)
        # -----------------------------------------------------------------
        if is_comparison:
            deed_type = self.extract_deed_type(query)
            if deed_type or any(word in query_lower for word in ['gift', 'sale', 'deed']):
                return Intent.COMPARE_DEED_TYPES, {
                    "deed_type": deed_type or "sale_transfer"
                }, QueryType.COMPARISON
        
        # -----------------------------------------------------------------
        # Registry queries
        # -----------------------------------------------------------------
        if any(word in query_lower for word in ['registry', 'land registry', 'register office', 'registration office', 'where to register']):
            district = self.extract_district(query)
            return Intent.FIND_REGISTRY, {"district": district or "Colombo"}, QueryType.FACTUAL
        
        # =================================================================
        # PRIORITY: Deed code routing
        # If a deed code is present in the query, route immediately based on
        # accompanying keywords. This prevents the person-name extractor or
        # other steps from hijacking queries like "tell about M 26028/77".
        # =================================================================
        code = self.extract_deed_code(query)
        if code:
            # Compliance / validity
            if any(word in query_lower for word in ['comply', 'compliant', 'valid', 'requirement', 'check']):
                return Intent.CHECK_COMPLIANCE, {"code": code}, query_type
            # Boundaries
            if any(word in query_lower for word in ['boundary', 'boundaries', 'border']):
                return Intent.FIND_BOUNDARIES, {"code": code}, query_type
            # Parties / who
            if any(word in query_lower for word in ['party', 'parties', 'who', 'vendor', 'vendee', 'donor', 'donee', 'owner', 'notary', 'involved']):
                return Intent.FIND_DEED_PARTIES, {"code": code}, query_type
            # Ownership chain / history
            if any(word in query_lower for word in ['history', 'chain', 'previous', 'prior', 'before']):
                return Intent.FIND_OWNERSHIP_CHAIN, {"code": code}, query_type
            # Governing law / statute
            if any(word in query_lower for word in ['statute', 'ordinance', 'govern', 'governing', 'legal basis']):
                return Intent.FIND_GOVERNING_LAW, {"code": code}, query_type
            # Default: full deed details
            return Intent.FIND_DEED_DETAILS, {"code": code}, query_type
        
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
            
            # Cost for this deed
            if any(word in query_lower for word in ['cost', 'stamp duty', 'fees']):
                return Intent.CALCULATE_TRANSFER_COST, {
                    "deed_code": context["deed_code"],
                    "deed_type": context.get("deed_type", "sale_transfer")
                }, QueryType.FACTUAL
            
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
            
            # Section-specific query
            section_match = re.search(r'section\s*(\d+)', query_lower)
            if section_match:
                return Intent.FIND_SECTION, {"query": query, "section_num": section_match.group(1)}, query_type
            
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
        # STEP 7: Amount/price queries (deeds by amount)
        # =================================================================
        if any(word in query_lower for word in ['expensive', 'costly', 'highest price', 'highest value']):
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
        if any(word in query_lower for word in ['lot', 'parcel', 'plan']):
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