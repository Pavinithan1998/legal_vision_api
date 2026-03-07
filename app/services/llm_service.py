"""
UPDATED LLM Reasoning Service
==============================

REPLACE your existing app/services/llm_service.py with this file.

Key improvements:
1. Better system prompt with actual data knowledge
2. Cost calculation logic built-in
3. Better citation formatting
4. Specific instructions for different query types
"""

from typing import Dict, List, Optional, Any
from openai import OpenAI
from app.core.config import settings
from app.utils.intent_detection import QueryType


class LLMReasoningService:
    """
    Service for generating legal reasoning responses using GPT-4o.
    Supports IRAC analysis, chain-of-thought reasoning, and natural language responses.
    """
    
    def __init__(self):
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.model = settings.OPENAI_MODEL
        self.temperature = settings.OPENAI_TEMPERATURE
        self.max_tokens = settings.OPENAI_MAX_TOKENS
        
        self.system_prompt = self._build_system_prompt()
    
    def _build_system_prompt(self) -> str:
        """Build comprehensive system prompt with REAL data knowledge."""
        return """You are an expert legal assistant specialized in Sri Lankan property law.
You provide accurate, well-reasoned legal analysis based on Sri Lankan statutes and legal principles.

═══════════════════════════════════════════════════════════════════════════════
KNOWLEDGE BASE - USE THIS DATA IN YOUR ANSWERS
═══════════════════════════════════════════════════════════════════════════════

GOVERNING STATUTES:
1. Prevention of Frauds Ordinance (1840) - PFO
   - Section 2: All deeds affecting immovable property must be in writing, signed, notarially attested, with 2 witnesses
   - Section 7: Leases >1 month must be notarially executed
   - Section 18: Contracts for land must be in writing and signed

2. Registration of Documents Ordinance (1927) - RDO  
   - Section 7: Unregistered instruments void against third parties with registered interests
   - Section 8: Registration within 3 months of execution
   - Section 14: Must register in proper folio for priority
   - Section 32: Caveats to protect interests

3. Prescription Ordinance (1871) - PO
   - Section 3: 10 years adverse possession gives title
   - Section 15: Does NOT apply to state land

4. Stamp Duty Act (1982) - SDA
   - Property transfers: 4% (LKR 4 per LKR 100)
   - Mortgages: 0.5%
   - Gifts between family: EXEMPTED
   - Leases: LKR 10 per LKR 1,000 annual rent

5. Other Key Laws:
   - Partition Act (1977) - court-ordered division of co-owned land
   - Mortgage Act (1949) - mortgage requirements
   - Notaries Ordinance (1907) - notary duties
   - Land Restrictions Act (2014) - 100% tax on foreign ownership

═══════════════════════════════════════════════════════════════════════════════
DEED REQUIREMENTS & COSTS
═══════════════════════════════════════════════════════════════════════════════

SALE/TRANSFER DEED:
- Requirements: Written, notarially attested, 2 witnesses, NIC of parties, property description with boundaries, survey plan reference, consideration stated, prior deed reference
- Stamp Duty: 4% of consideration OR market value (whichever is higher)
- Registration Fee: ~0.25% of value
- Notary Fee: 0.5-1% (negotiable)
- Time Limit: Register within 3 months
- Governing Laws: Prevention of Frauds Ordinance, Registration of Documents Ordinance

GIFT DEED:
- Requirements: Same as sale deed, plus acceptance by donee
- Stamp Duty: EXEMPTED for gifts between spouses, parents/children, siblings
- Registration Fee: ~0.25% of market value
- Notary Fee: 0.5-1%
- Time Limit: Register within 3 months
- Note: No consideration required, but market value assessed for registration

MORTGAGE DEED:
- Requirements: Written, notarially attested, property described, loan amount, interest rate, repayment terms
- Stamp Duty: 0.5% of loan amount
- Governing Law: Mortgage Act

LEASE DEED (>1 year):
- Requirements: Notarially executed, property described, lease period, rent stated
- Stamp Duty: LKR 10 per LKR 1,000 of annual rent
- Registration: Required for leases >1 year

═══════════════════════════════════════════════════════════════════════════════
LAND PRICES (2024-2025 Data) - USE THESE FOR ESTIMATES
═══════════════════════════════════════════════════════════════════════════════

COLOMBO DISTRICT:
- Colombo 3 (Kollupitiya): LKR 35,000,000 per perch
- Colombo 5 (Havelock): LKR 28,000,000 per perch
- Colombo 7 (Cinnamon Gardens): LKR 32,000,000 per perch
- Rajagiriya: LKR 8,000,000 per perch (+12% trend)
- Nugegoda: LKR 5,000,000 per perch (+15% trend)
- Dehiwala: LKR 6,000,000 per perch (+10% trend)
- Maharagama: LKR 3,000,000 per perch (+20% trend)
- Athurugiriya: LKR 1,800,000 per perch (+28% trend)
- Homagama: LKR 1,200,000 per perch (+25% trend)
- Kaduwela: LKR 2,000,000 per perch (+22% trend)
- Kolonnawa: LKR 2,800,000 per perch (+21% trend)
- Moratuwa: LKR 3,500,000 per perch (+18% trend)

GAMPAHA DISTRICT:
- Negombo: LKR 4,000,000 per perch
- Kelaniya: LKR 3,800,000 per perch
- Ja-Ela: LKR 2,200,000 per perch
- Yakkala: LKR 1,200,000 per perch (+55% trend)

KANDY DISTRICT:
- Kandy Town: LKR 5,000,000 per perch
- Peradeniya: LKR 3,000,000 per perch
- Katugastota: LKR 2,000,000 per perch

GALLE DISTRICT:
- Galle Fort: LKR 12,000,000 per perch
- Unawatuna: LKR 8,000,000 per perch
- Hikkaduwa: LKR 6,000,000 per perch

SOUTHERN DISTRICT:
- Matara: LKR 1,800,000 per perch
- Mirissa: LKR 5,000,000 per perch

Note: 1 perch = 25.29 sq meters = 272.25 sq feet

═══════════════════════════════════════════════════════════════════════════════
LEGAL PRINCIPLES - CITE THESE WHEN RELEVANT
═══════════════════════════════════════════════════════════════════════════════

1. Nemo Dat Quod Non Habet - "No one can give what they do not have"
   → Buyer cannot get better title than seller had
   
2. Caveat Emptor - "Let the buyer beware"
   → Buyer responsible for verifying title before purchase
   
3. Qui Prior Est Tempore Potior Est Jure - "First in time, stronger in right"
   → Priority based on registration date (Section 7, RDO)

═══════════════════════════════════════════════════════════════════════════════
RESPONSE GUIDELINES
═══════════════════════════════════════════════════════════════════════════════

FOR COST/PRICE QUESTIONS:
1. Give SPECIFIC NUMBERS, not vague ranges
2. Break down: Stamp Duty + Registration Fee + Notary Fee = Total
3. Show calculation: "For a LKR 10,000,000 sale: 4% stamp duty = LKR 400,000"
4. Cite the Stamp Duty Act and relevant rates
5. Mention the 3-month registration deadline

FOR DEED TYPE COMPARISON (Gift vs Sale):
1. Compare stamp duty (Gift exempted vs Sale 4%)
2. Compare requirements (same except consideration)
3. Give cost comparison with actual numbers
4. Recommend which is better for family transfers
5. Cite Prevention of Frauds Ordinance

FOR LEGAL VALIDITY QUESTIONS:
1. Use IRAC format
2. Cite specific sections (e.g., "Section 2 of the Prevention of Frauds Ordinance")
3. Quote the actual legal requirement
4. Apply to the specific facts
5. Give clear conclusion

FOR LAND PRICE QUESTIONS:
1. Give actual price data from the area
2. Include the trend (e.g., "+28% year-on-year")
3. Compare with nearby areas if helpful
4. Explain factors affecting price

ALWAYS:
- Be specific with numbers and citations
- Reference actual statute sections
- Use the knowledge graph data provided
- Format monetary values with LKR and commas (LKR 1,500,000)
- Mention relevant time limits (3 months for registration)
"""
    
    def _format_graph_data(self, data: List[Dict], intent: str) -> str:
        """Format graph data for LLM context with better structure."""
        if not data:
            return "No specific data found in the knowledge graph for this query."
        
        context = f"═══ KNOWLEDGE GRAPH RESULTS ({intent}) ═══\n\n"
        
        for i, record in enumerate(data, 1):
            context += f"【Result {i}】\n"
            for key, value in record.items():
                if value is not None and value != "" and value != []:
                    # Format different types of data
                    if key == "parties" and isinstance(value, list):
                        context += f"  • Parties:\n"
                        for party in value:
                            if isinstance(party, dict):
                                name = party.get('name', 'Unknown')
                                role = party.get('role', 'Unknown')
                                context += f"      - {role.upper()}: {name}\n"
                    elif key == "requirements" and isinstance(value, list):
                        context += f"  • Requirements:\n"
                        for req in value:
                            context += f"      ✓ {req}\n"
                    elif key == "key_provisions" and isinstance(value, list):
                        context += f"  • Key Provisions:\n"
                        for prov in value:
                            context += f"      → {prov}\n"
                    elif key == "governing_statutes" and isinstance(value, list):
                        statutes = [str(v.get('name', v) if isinstance(v, dict) else v) for v in value]
                        context += f"  • Governing Laws: {', '.join(statutes)}\n"
                    elif key in ["stamp_duty", "stamp_duty_rule"]:
                        context += f"  • STAMP DUTY: {value}\n"
                    elif key in ["amount", "consideration_lkr", "avg_price", "avg_price_per_perch"]:
                        if isinstance(value, (int, float)):
                            context += f"  • {key.replace('_', ' ').title()}: LKR {value:,.0f}\n"
                        else:
                            context += f"  • {key.replace('_', ' ').title()}: {value}\n"
                    elif key == "content" and len(str(value)) > 200:
                        context += f"  • {key.title()}:\n      \"{value[:500]}...\"\n"
                    elif isinstance(value, dict):
                        context += f"  • {key.replace('_', ' ').title()}: {value}\n"
                    else:
                        context += f"  • {key.replace('_', ' ').title()}: {value}\n"
            context += "\n"
        
        return context
    
    def _get_response_instruction(self, query_type: QueryType, query: str = "") -> str:
        """Get specific instructions based on query type and content."""
        
        query_lower = query.lower() if query else ""
        
        # Detect cost/price questions
        is_cost_question = any(word in query_lower for word in 
            ['cost', 'price', 'how much', 'stamp duty', 'fee', 'charge', 'pay', 'expense', 'budget'])
        
        is_comparison = any(word in query_lower for word in 
            ['compare', 'vs', 'versus', 'difference', 'better', 'which', 'or'])
        
        is_transfer_question = any(word in query_lower for word in 
            ['transfer', 'give', 'son', 'daughter', 'wife', 'husband', 'parent', 'child', 'family'])
        
        # Cost calculation questions
        if is_cost_question:
            return """
PROVIDE SPECIFIC COST BREAKDOWN:

1. **Stamp Duty**: Calculate exact amount
   - Sale deed: 4% of consideration/market value
   - Gift deed: EXEMPTED for family transfers
   - Mortgage: 0.5% of loan amount
   
2. **Registration Fee**: ~0.25% of value

3. **Notary Fee**: Estimate 0.5-1% (negotiable)

4. **TOTAL**: Add all costs

Example format:
"For a property valued at LKR 10,000,000:
- Stamp Duty (4%): LKR 400,000
- Registration Fee (~0.25%): LKR 25,000  
- Notary Fee (~0.75%): LKR 75,000
- **TOTAL ESTIMATED COST: LKR 500,000**"

ALWAYS cite: Stamp Duty Act, Registration of Documents Ordinance
ALWAYS mention: 3-month registration deadline"""

        # Deed type comparison (Gift vs Sale)
        if is_comparison and is_transfer_question:
            return """
COMPARE DEED TYPES WITH SPECIFIC COSTS:

**GIFT DEED (Recommended for family)**
- Stamp Duty: EXEMPTED (for spouse, children, parents, siblings)
- Only pay: Registration fee + Notary fee
- Example: LKR 10M property → ~LKR 100,000 total

**SALE DEED**  
- Stamp Duty: 4% of value
- Example: LKR 10M property → ~LKR 500,000 total

**RECOMMENDATION**: For transfers to family members, GIFT DEED saves ~80% in costs.

**LEGAL REQUIREMENTS (same for both)**:
Per Section 2 of Prevention of Frauds Ordinance:
- Written document
- Notarially attested  
- Two witnesses
- Property described with boundaries
- Register within 3 months (Section 8, RDO)

Give a clear recommendation with cost comparison."""

        # Legal reasoning
        if query_type == QueryType.LEGAL_REASONING:
            return """
USE IRAC FORMAT:

**ISSUE:** State the legal question clearly

**RULE:** Cite the specific statute and section
- Quote: "Section X of [Ordinance] states that..."
- Include the actual legal requirement

**APPLICATION:** Apply the rule to the facts
- Be specific about how the law applies
- Address each requirement

**CONCLUSION:** Clear answer with confidence level

Always cite:
- Prevention of Frauds Ordinance (Section 2 for property deeds)
- Registration of Documents Ordinance (Section 7-8 for registration)
- Relevant specific sections"""

        # Compliance questions
        if query_type == QueryType.COMPLIANCE:
            return """
CHECK EACH REQUIREMENT:

For [Deed Type], the law requires:

✓ Written document - [Met/Not Met]
✓ Notarially attested - [Met/Not Met]  
✓ Two witnesses - [Met/Not Met]
✓ Parties identified by NIC - [Met/Not Met]
✓ Property described with boundaries - [Met/Not Met]
✓ Registered within 3 months - [Met/Not Met]

Cite Section 2 of Prevention of Frauds Ordinance for requirements.
Cite Section 7 of Registration of Documents Ordinance for registration effect."""

        # Definition questions
        if query_type == QueryType.DEFINITION:
            return """
PROVIDE CLEAR DEFINITION:

1. Legal definition from the source statute
2. Practical meaning in plain language
3. Example of how it applies
4. Source citation

Keep it concise but complete."""

        # Default factual
        return """
Provide a direct, specific answer:
- Use actual numbers from the data
- Cite relevant statutes
- Be precise, not vague
- If data is missing, say what would be needed"""

    def generate_response(
        self,
        user_query: str,
        graph_data: List[Dict],
        intent: str,
        query_type: QueryType,
        conversation_history: List[Dict] = None,
        include_reasoning: bool = True
    ) -> Dict[str, Any]:
        """
        Generate a response using GPT-4o with legal reasoning.
        """
        # Format graph data
        context = self._format_graph_data(graph_data, intent)
        
        # Get specific instructions based on query type and content
        response_instruction = self._get_response_instruction(query_type, user_query)
        
        # Build conversation context
        history_text = ""
        if conversation_history:
            history_text = "\n\n【Previous Context】\n"
            for h in conversation_history[-3:]:
                history_text += f"• User asked: {h.get('query', '')[:100]}...\n"
        
        # Build the prompt
        user_message = f"""【USER QUESTION】
{user_query}
{history_text}

【RETRIEVED DATA FROM KNOWLEDGE GRAPH】
{context}

【INSTRUCTIONS】
{response_instruction}

{"Include step-by-step reasoning before your final answer." if include_reasoning else ""}

Provide a helpful, SPECIFIC answer. Use actual numbers and cite actual statute sections.
If the knowledge graph data has the information, USE IT. 
If data is missing, use the baseline knowledge from the system prompt."""
        
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_message}
        ]
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            answer = response.choices[0].message.content
            
            # Parse IRAC analysis if present
            irac_analysis = None
            if query_type in [QueryType.LEGAL_REASONING, QueryType.COMPLIANCE]:
                irac_analysis = self._extract_irac(answer)
            
            # Extract reasoning steps
            reasoning_steps = None
            if include_reasoning:
                reasoning_steps = self._extract_reasoning_steps(answer)
            
            # Extract referenced statutes
            referenced_statutes = self._extract_statute_references(answer, graph_data)
            
            # Calculate confidence based on data availability
            confidence = self._calculate_confidence(graph_data, answer, user_query)
            
            return {
                "answer": answer,
                "reasoning_steps": reasoning_steps,
                "irac_analysis": irac_analysis,
                "sources": self._extract_sources(graph_data),
                "related_statutes": referenced_statutes,
                "confidence": confidence
            }
            
        except Exception as e:
            return {
                "answer": f"I apologize, but I encountered an error generating the response: {str(e)}. Please try rephrasing your question.",
                "reasoning_steps": None,
                "irac_analysis": None,
                "sources": [],
                "related_statutes": [],
                "confidence": 0.0
            }
    
    def _extract_irac(self, answer: str) -> Optional[Dict[str, str]]:
        """Extract IRAC components from the answer."""
        import re
        
        irac = {}
        
        patterns = {
            'issue': r'\*?\*?ISSUE:?\*?\*?\s*(.+?)(?=\*?\*?RULE|\n\n|$)',
            'rule': r'\*?\*?RULE:?\*?\*?\s*(.+?)(?=\*?\*?APPLICATION|\n\n|$)',
            'application': r'\*?\*?APPLICATION:?\*?\*?\s*(.+?)(?=\*?\*?CONCLUSION|\n\n|$)',
            'conclusion': r'\*?\*?CONCLUSION:?\*?\*?\s*(.+?)(?=\n\n|$)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, answer, re.IGNORECASE | re.DOTALL)
            if match:
                irac[key] = match.group(1).strip()
        
        return irac if len(irac) >= 2 else None
    
    def _extract_reasoning_steps(self, answer: str) -> Optional[List[Dict]]:
        """Extract reasoning steps from the answer."""
        import re
        
        steps = []
        
        # Look for numbered steps or bullet points
        patterns = [
            r'(?:Step\s*)?(\d+)[.:]\s*(.+?)(?=(?:Step\s*)?\d+[.:]|\n\n|$)',
            r'[•\-]\s*(.+?)(?=[•\-]|\n\n|$)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, answer, re.IGNORECASE | re.DOTALL)
            if matches:
                for i, match in enumerate(matches, 1):
                    if isinstance(match, tuple):
                        text = match[1].strip()
                    else:
                        text = match.strip()
                    
                    if len(text) > 10:  # Filter out very short matches
                        steps.append({
                            "step": i,
                            "text": text[:200],
                            "legal_basis": self._extract_legal_basis(text)
                        })
                break
        
        return steps if steps else None
    
    def _extract_legal_basis(self, text: str) -> Optional[str]:
        """Extract legal basis (statute/section) from text."""
        import re
        
        patterns = [
            r'(Section\s+\d+[A-Za-z]?\s+of\s+(?:the\s+)?[\w\s]+(?:Ordinance|Act))',
            r'(Prevention of Frauds Ordinance)',
            r'(Registration of Documents Ordinance)',
            r'(Stamp Duty Act)',
            r'(Prescription Ordinance)',
            r'(Mortgage Act)',
            r'(Partition Act)',
            r'(Notaries Ordinance)',
            r'(Section\s+\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def _extract_sources(self, graph_data: List[Dict]) -> List[str]:
        """Extract source references from graph data."""
        sources = set()
        
        for record in graph_data:
            if record.get('deed_code'):
                sources.add(f"Deed: {record['deed_code']}")
            if record.get('statute_name'):
                sources.add(f"Statute: {record['statute_name']}")
            if record.get('short_name'):
                sources.add(f"Statute: {record['short_name']}")
            if record.get('term'):
                sources.add(f"Definition: {record['term']}")
            if record.get('principle_name'):
                sources.add(f"Principle: {record['principle_name']}")
            if record.get('section_number'):
                statute = record.get('statute_short_name', record.get('statute_name', ''))
                sources.add(f"Section: {record['section_number']} ({statute})")
            if record.get('area'):
                sources.add(f"Price Data: {record['area']}")
            if record.get('deed_type') and record.get('stamp_duty'):
                sources.add(f"Requirement: {record['deed_type']}")
        
        return list(sources)
    
    def _extract_statute_references(self, answer: str, graph_data: List[Dict]) -> List[str]:
        """Extract referenced statutes from answer and data."""
        statutes = set()
        
        # From graph data
        for record in graph_data:
            if record.get('governing_statutes'):
                for stat in record['governing_statutes']:
                    if isinstance(stat, dict):
                        statutes.add(stat.get('name', ''))
                    else:
                        statutes.add(str(stat))
            if record.get('statute_name'):
                statutes.add(record['statute_name'])
            if record.get('short_name'):
                statutes.add(record['short_name'])
        
        # From answer text
        statute_names = [
            ('Prevention of Frauds Ordinance', 'PFO'),
            ('Registration of Documents Ordinance', 'RDO'),
            ('Stamp Duty Act', 'SDA'),
            ('Prescription Ordinance', 'PO'),
            ('Mortgage Act', 'MA'),
            ('Partition Act', 'PA'),
            ('Notaries Ordinance', 'NO'),
            ('Rent Act', 'RA'),
            ('Wills Ordinance', 'WO'),
            ('Land Restrictions Act', 'LRA'),
        ]
        
        answer_lower = answer.lower()
        for full_name, short in statute_names:
            if full_name.lower() in answer_lower or short.lower() in answer_lower:
                statutes.add(full_name)
        
        return [s for s in statutes if s]
    
    def _calculate_confidence(self, graph_data: List[Dict], answer: str, query: str) -> float:
        """Calculate confidence score based on data availability and answer quality."""
        confidence = 0.4  # Base confidence
        
        # Increase for graph data presence
        if graph_data:
            confidence += 0.15
            if len(graph_data) >= 2:
                confidence += 0.1
        
        # Increase if answer cites specific data
        if any(record.get('deed_code') and str(record['deed_code']) in answer for record in graph_data):
            confidence += 0.1
        
        # Increase for statute citations in answer
        statute_keywords = ['section', 'ordinance', 'act', 'statute']
        if any(kw in answer.lower() for kw in statute_keywords):
            confidence += 0.1
        
        # Increase for specific numbers in cost questions
        if any(word in query.lower() for word in ['cost', 'price', 'how much', 'stamp duty']):
            if 'LKR' in answer or '%' in answer:
                confidence += 0.1
        
        # Increase for IRAC structure in legal questions
        if 'ISSUE' in answer.upper() and 'CONCLUSION' in answer.upper():
            confidence += 0.05
        
        return min(confidence, 0.95)


# Create singleton instance
llm_service = LLMReasoningService()
