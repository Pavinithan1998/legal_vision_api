"""
LLM Reasoning Service
Handles GPT-4o integration for legal reasoning and response generation
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
        """Build comprehensive system prompt for legal reasoning."""
        return """You are an expert legal assistant specialized in Sri Lankan property law.
You provide accurate, well-reasoned legal analysis based on Sri Lankan statutes and legal principles.

KNOWLEDGE BASE:
You have access to a knowledge graph containing:
- Property deeds (sales, gifts, wills, mortgages, leases, partitions)
- Parties (vendors, vendees, donors, donees, testators, notaries)
- Properties (parcels with lot numbers, plans, extents, boundaries)
- Locations (districts, provinces, registry offices)
- Sri Lankan Statutes (Prevention of Frauds Ordinance, Registration of Documents Ordinance, etc.)
- Legal Sections (specific provisions from statutes)
- Legal Definitions (property law terminology)
- Legal Principles (Nemo Dat, Caveat Emptor, etc.)
- Deed Requirements (what each deed type needs to be valid)

RESPONSE GUIDELINES:

1. FOR FACTUAL QUERIES (what, who, where, when):
   - Provide direct, specific answers from the graph data
   - Reference deed numbers, party names, dates, and amounts
   - Format boundaries as: North: ..., South: ..., East: ..., West: ...
   - Format amounts with LKR prefix and thousand separators

2. FOR LEGAL REASONING QUERIES (why, how, is it valid, legal requirements):
   - Use IRAC format:
     * ISSUE: State the legal question clearly
     * RULE: Cite relevant statute/section/principle
     * APPLICATION: Apply the rule to the specific facts
     * CONCLUSION: State your conclusion with confidence level
   - Reference specific statutes by name and section
   - Explain legal concepts in accessible terms

3. FOR COMPLIANCE QUERIES (is this deed valid, what's required):
   - List specific requirements from DeedRequirement data
   - Check each requirement against the actual deed data
   - Identify missing elements or potential issues
   - Cite governing statutes for each requirement

4. FOR DEFINITION QUERIES (what is, define, meaning):
   - Provide the legal definition from the data
   - Include the source of the definition
   - Give practical examples when helpful
   - Relate to Sri Lankan property law context

5. ALWAYS:
   - Be accurate and cite sources from the provided data
   - Acknowledge when data is insufficient
   - Distinguish between facts and legal interpretation
   - Use professional but accessible language
   - Reference Sri Lankan law specifically
   - Do not make up information not in the data"""
    
    def _format_graph_data(self, data: List[Dict], intent: str) -> str:
        """Format graph data for LLM context."""
        if not data:
            return "No relevant data found in the knowledge graph."
        
        context = f"Knowledge Graph Results ({intent}):\n"
        context += "-" * 50 + "\n"
        
        for i, record in enumerate(data, 1):
            context += f"\nResult {i}:\n"
            for key, value in record.items():
                if value is not None and value != "" and value != []:
                    if key == "parties" and isinstance(value, list):
                        context += f"  {key}:\n"
                        for party in value:
                            if isinstance(party, dict):
                                name = party.get('name', 'Unknown')
                                role = party.get('role', 'Unknown role')
                                context += f"    - {name}: {role}\n"
                    elif key == "governing_statutes" and isinstance(value, list):
                        context += f"  {key}: {', '.join(str(v) for v in value)}\n"
                    elif key == "key_provisions" and isinstance(value, list):
                        context += f"  {key}:\n"
                        for provision in value:
                            context += f"    - {provision}\n"
                    elif key == "sections" and isinstance(value, list):
                        context += f"  {key}:\n"
                        for sec in value:
                            if isinstance(sec, dict):
                                context += f"    - {sec.get('section', 'N/A')}: {sec.get('title', 'N/A')}\n"
                    elif isinstance(value, dict):
                        context += f"  {key}: {value}\n"
                    else:
                        context += f"  {key}: {value}\n"
        
        return context
    
    def _get_response_instruction(self, query_type: QueryType) -> str:
        """Get specific instructions based on query type."""
        instructions = {
            QueryType.FACTUAL: """
Provide a direct, factual answer based on the data.
Be specific and reference actual values from the results.
Format any boundaries clearly (North, South, East, West).
Format monetary amounts with LKR prefix.""",
            
            QueryType.LEGAL_REASONING: """
Provide a legal analysis using IRAC format:

**ISSUE:** [State the legal question]

**RULE:** [Cite the relevant Sri Lankan statute, section, or principle]

**APPLICATION:** [Apply the rule to the specific facts]

**CONCLUSION:** [State your conclusion]

Reference specific statutes and sections from the data.""",
            
            QueryType.DEFINITION: """
Provide the legal definition clearly.
Include the source of the definition.
Explain how this term applies in Sri Lankan property law.
Give a practical example if helpful.""",
            
            QueryType.COMPLIANCE: """
Analyze the deed's compliance with Sri Lankan law:

1. List the requirements for this deed type
2. Check each requirement against the deed data
3. Identify any missing or incomplete items
4. Cite the governing statutes
5. Provide a compliance assessment (compliant/non-compliant/needs review)
6. Suggest remediation if needed""",
            
            QueryType.COMPARISON: """
Compare the items clearly:
1. List similarities
2. List differences
3. Explain when each applies
4. Reference relevant Sri Lankan law"""
        }
        
        return instructions.get(query_type, instructions[QueryType.FACTUAL])
    
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
        
        Args:
            user_query: The user's question
            graph_data: Data retrieved from Neo4j
            intent: Detected intent
            query_type: Type of query (factual, legal_reasoning, etc.)
            conversation_history: Previous conversation for context
            include_reasoning: Whether to include reasoning steps
            
        Returns:
            Dict with answer, reasoning_steps, irac_analysis, etc.
        """
        # Format graph data
        context = self._format_graph_data(graph_data, intent)
        
        # Build conversation history context
        history_text = ""
        if conversation_history:
            history_text = "\n\nRecent conversation:\n"
            for h in conversation_history[-3:]:
                history_text += f"- User: {h.get('query', '')[:100]}...\n"
        
        # Get response instruction based on query type
        response_instruction = self._get_response_instruction(query_type)
        
        # Build the prompt
        user_message = f"""User Question: {user_query}
{history_text}
Retrieved Knowledge Graph Data:
{context}

Instructions:
{response_instruction}

{"Include step-by-step reasoning before your final answer." if include_reasoning else ""}
Provide a helpful, accurate answer based on the data above.
If the data is insufficient, acknowledge that and suggest what information might help."""
        
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
            if query_type == QueryType.LEGAL_REASONING or query_type == QueryType.COMPLIANCE:
                irac_analysis = self._extract_irac(answer)
            
            # Extract reasoning steps if present
            reasoning_steps = None
            if include_reasoning:
                reasoning_steps = self._extract_reasoning_steps(answer)
            
            # Extract referenced statutes
            referenced_statutes = self._extract_statute_references(answer, graph_data)
            
            return {
                "answer": answer,
                "reasoning_steps": reasoning_steps,
                "irac_analysis": irac_analysis,
                "sources": self._extract_sources(graph_data),
                "related_statutes": referenced_statutes,
                "confidence": self._calculate_confidence(graph_data, answer)
            }
            
        except Exception as e:
            return {
                "answer": f"Error generating response: {str(e)}",
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
        
        # Try to extract ISSUE
        issue_match = re.search(r'\*?\*?ISSUE:?\*?\*?\s*(.+?)(?=\*?\*?RULE|\n\n|$)', answer, re.IGNORECASE | re.DOTALL)
        if issue_match:
            irac['issue'] = issue_match.group(1).strip()
        
        # Try to extract RULE
        rule_match = re.search(r'\*?\*?RULE:?\*?\*?\s*(.+?)(?=\*?\*?APPLICATION|\n\n|$)', answer, re.IGNORECASE | re.DOTALL)
        if rule_match:
            irac['rule'] = rule_match.group(1).strip()
        
        # Try to extract APPLICATION
        app_match = re.search(r'\*?\*?APPLICATION:?\*?\*?\s*(.+?)(?=\*?\*?CONCLUSION|\n\n|$)', answer, re.IGNORECASE | re.DOTALL)
        if app_match:
            irac['application'] = app_match.group(1).strip()
        
        # Try to extract CONCLUSION
        conc_match = re.search(r'\*?\*?CONCLUSION:?\*?\*?\s*(.+?)(?=\n\n|$)', answer, re.IGNORECASE | re.DOTALL)
        if conc_match:
            irac['conclusion'] = conc_match.group(1).strip()
        
        return irac if len(irac) >= 2 else None
    
    def _extract_reasoning_steps(self, answer: str) -> Optional[List[Dict]]:
        """Extract reasoning steps from the answer."""
        import re
        
        steps = []
        
        # Look for numbered steps
        step_pattern = r'(?:Step\s*)?(\d+)[.:]\s*(.+?)(?=(?:Step\s*)?\d+[.:]|\n\n|$)'
        matches = re.findall(step_pattern, answer, re.IGNORECASE | re.DOTALL)
        
        for num, text in matches:
            steps.append({
                "step": int(num),
                "text": text.strip(),
                "legal_basis": self._extract_legal_basis(text)
            })
        
        return steps if steps else None
    
    def _extract_legal_basis(self, text: str) -> Optional[str]:
        """Extract legal basis (statute/section) from text."""
        import re
        
        # Look for statute references
        patterns = [
            r'(Prevention of Frauds Ordinance)',
            r'(Registration of Documents Ordinance)',
            r'(Mortgage Act)',
            r'(Partition Act)',
            r'(Registration of Title Act)',
            r'(Prescription Ordinance)',
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
        sources = []
        
        for record in graph_data:
            if record.get('deed_code'):
                sources.append(f"Deed: {record['deed_code']}")
            if record.get('statute_name'):
                sources.append(f"Statute: {record['statute_name']}")
            if record.get('term'):
                sources.append(f"Definition: {record['term']}")
            if record.get('principle_name'):
                sources.append(f"Principle: {record['principle_name']}")
        
        return list(set(sources))
    
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
        
        # From answer text
        statute_names = [
            'Prevention of Frauds Ordinance',
            'Registration of Documents Ordinance',
            'Mortgage Act',
            'Partition Act',
            'Registration of Title Act',
            'Prescription Ordinance',
            'Notaries Ordinance',
            'Rent Act',
            'Stamp Duty Act',
            'Wills Ordinance'
        ]
        
        for name in statute_names:
            if name.lower() in answer.lower():
                statutes.add(name)
        
        return [s for s in statutes if s]
    
    def _calculate_confidence(self, graph_data: List[Dict], answer: str) -> float:
        """Calculate confidence score based on data availability."""
        if not graph_data:
            return 0.3
        
        # Base confidence on data presence
        confidence = 0.5
        
        # Increase for more data
        if len(graph_data) > 0:
            confidence += 0.2
        if len(graph_data) > 2:
            confidence += 0.1
        
        # Increase if answer references specific data
        if any(record.get('deed_code') and record['deed_code'] in answer for record in graph_data):
            confidence += 0.1
        
        # Increase if statutes are referenced
        if any(record.get('governing_statutes') for record in graph_data):
            confidence += 0.1
        
        return min(confidence, 1.0)


# Create singleton instance
llm_service = LLMReasoningService()
