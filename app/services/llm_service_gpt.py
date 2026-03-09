"""
UPDATED LLM Reasoning Service - Using Deployed LegalVision Model
=================================================================

This version uses the fine-tuned LegalVision model deployed on HuggingFace
Inference Endpoints instead of OpenAI GPT.

Endpoint: https://scva0fjg3t7o0ykh.us-east-1.aws.endpoints.huggingface.cloud
"""

import requests
import re
from typing import Dict, List, Optional, Any
from app.core.config import settings
from app.utils.intent_detection import QueryType


class LLMReasoningService:
    """
    Service for generating legal reasoning responses using the deployed LegalVision model.
    Supports IRAC analysis, chain-of-thought reasoning, and natural language responses.
    """
    
    def __init__(self):
        # LegalVision Endpoint Configuration
        self.endpoint_url = settings.LEGALVISION_ENDPOINT_URL  # Add this to your config
        self.hf_token = settings.HF_TOKEN  # Add this to your config
        
        # Fallback to hardcoded values if not in settings
        if not hasattr(settings, 'LEGALVISION_ENDPOINT_URL') or not self.endpoint_url:
            self.endpoint_url = "https://scva0fjg3t7o0ykh.us-east-1.aws.endpoints.huggingface.cloud"
        
        if not hasattr(settings, 'HF_TOKEN') or not self.hf_token:
            self.hf_token = None  # Will need to be set
        
        self.headers = {
            "Content-Type": "application/json"
        }
        if self.hf_token:
            self.headers["Authorization"] = f"Bearer {self.hf_token}"
        
        # Generation parameters
        self.max_tokens = 1024
        self.temperature = 0.7
        self.top_p = 0.9
        
        self.system_prompt = self._build_system_prompt()
    
    def _build_system_prompt(self) -> str:
        """Build comprehensive system prompt with legal knowledge."""
        return """You are an expert legal assistant specialized in Sri Lankan property law.
You provide accurate, well-reasoned legal analysis based on Sri Lankan statutes and legal principles.

GOVERNING STATUTES:
1. Prevention of Frauds Ordinance (1840) - PFO
   - Section 2: All deeds affecting immovable property must be in writing, signed, notarially attested, with 2 witnesses
   - Section 7: Leases >1 month must be notarially executed

2. Registration of Documents Ordinance (1927) - RDO  
   - Section 7: Unregistered instruments void against third parties
   - Section 8: Registration within 3 months of execution

3. Prescription Ordinance (1871) - PO
   - Section 3: 10 years adverse possession gives title

4. Stamp Duty Act (1982) - SDA
   - Property transfers: 4% 
   - Mortgages: 0.5%
   - Gifts between family: EXEMPTED

5. Other Key Laws:
   - Partition Act (1977)
   - Mortgage Act (1949)
   - Notaries Ordinance (1907)
   - Land Restrictions Act (2014) - 100% tax on foreign ownership

RESPONSE FORMAT:
1. A clear answer
2. Step-by-step reasoning with legal basis
3. IRAC analysis (Issue, Rule, Application, Conclusion)
4. Relevant legal references
5. Practical examples where helpful

Always cite specific statute sections and provide accurate legal information."""

    def _call_legalvision_endpoint(self, prompt: str) -> str:
        """Call the deployed LegalVision model endpoint."""
        
        payload = {
            "inputs": prompt,  # Use 'inputs' not 'messages'
            "parameters": {
                "max_new_tokens": self.max_tokens,
                "temperature": self.temperature,
                "top_p": self.top_p
            }
        }
        
        try:
            response = requests.post(
                self.endpoint_url,
                headers=self.headers,
                json=payload,
                timeout=120
            )
            
            if response.status_code == 200:
                result = response.json()
                if isinstance(result, dict):
                    return result.get("generated_text", str(result))
                elif isinstance(result, list) and len(result) > 0:
                    return result[0].get("generated_text", str(result))
                return str(result)
            else:
                return f"Error: {response.status_code}"
                
        except Exception as e:
            return f"Error: {str(e)}"

    def _format_graph_data(self, data: List[Dict], intent: str) -> str:
        """Format graph data for model context."""
        if not data:
            return "No specific data found in the knowledge graph for this query."
        
        context = f"=== KNOWLEDGE GRAPH DATA ({intent}) ===\n\n"
        
        for i, record in enumerate(data, 1):
            context += f"[Result {i}]\n"
            for key, value in record.items():
                if value is not None and value != "" and value != []:
                    if key == "parties" and isinstance(value, list):
                        context += f"  Parties:\n"
                        for party in value:
                            if isinstance(party, dict):
                                name = party.get('name', 'Unknown')
                                role = party.get('role', 'Unknown')
                                context += f"    - {role.upper()}: {name}\n"
                    elif key == "requirements" and isinstance(value, list):
                        context += f"  Requirements:\n"
                        for req in value:
                            context += f"    - {req}\n"
                    elif key == "governing_statutes" and isinstance(value, list):
                        statutes = [str(v.get('name', v) if isinstance(v, dict) else v) for v in value]
                        context += f"  Governing Laws: {', '.join(statutes)}\n"
                    elif key in ["amount", "consideration_lkr", "avg_price"]:
                        if isinstance(value, (int, float)):
                            context += f"  {key.replace('_', ' ').title()}: LKR {value:,.0f}\n"
                        else:
                            context += f"  {key.replace('_', ' ').title()}: {value}\n"
                    elif isinstance(value, dict):
                        context += f"  {key.replace('_', ' ').title()}: {value}\n"
                    else:
                        context += f"  {key.replace('_', ' ').title()}: {value}\n"
            context += "\n"
        
        return context

    def _build_prompt(self, user_query: str, graph_data: List[Dict], intent: str, query_type: QueryType) -> str:
        """Build the complete prompt for the model."""
        
        # Format graph data
        context = self._format_graph_data(graph_data, intent)
        
        # Get query-specific instructions
        instructions = self._get_response_instruction(query_type, user_query)
        
        # Build full prompt
        prompt = f"""{self.system_prompt}

{context}

QUESTION: {user_query}

{instructions}

Please provide a comprehensive answer with step-by-step reasoning and relevant statute citations."""
        
        return prompt

    def _get_response_instruction(self, query_type: QueryType, query: str = "") -> str:
        """Get specific instructions based on query type."""
        
        query_lower = query.lower() if query else ""
        
        is_cost_question = any(word in query_lower for word in 
            ['cost', 'price', 'how much', 'stamp duty', 'fee', 'charge', 'pay'])
        
        is_comparison = any(word in query_lower for word in 
            ['compare', 'vs', 'versus', 'difference', 'better', 'which'])
        
        if is_cost_question:
            return """PROVIDE SPECIFIC COST BREAKDOWN:
- Stamp Duty: Calculate exact amount (Sale: 4%, Gift to family: EXEMPTED, Mortgage: 0.5%)
- Registration Fee: ~0.25% of value
- Notary Fee: ~0.5-1%
- Show calculation with actual numbers
- Cite Stamp Duty Act"""

        if is_comparison:
            return """COMPARE WITH SPECIFIC DETAILS:
- List differences clearly
- Provide cost comparison with numbers
- Give recommendation
- Cite relevant statutes"""

        if query_type == QueryType.LEGAL_REASONING:
            return """USE IRAC FORMAT:
**ISSUE:** State the legal question
**RULE:** Cite specific statute sections
**APPLICATION:** Apply law to facts
**CONCLUSION:** Clear answer"""

        if query_type == QueryType.COMPLIANCE:
            return """CHECK EACH REQUIREMENT:
- Written document
- Notarially attested
- Two witnesses
- Registered within 3 months
Cite Section 2 of Prevention of Frauds Ordinance."""

        if query_type == QueryType.DEFINITION:
            return """PROVIDE:
1. Legal definition
2. Plain language meaning
3. Practical example
4. Source citation"""

        return "Provide a direct, specific answer with statute citations."

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
        Generate a response using the deployed LegalVision model.
        """
        
        # Build the prompt
        prompt = self._build_prompt(user_query, graph_data, intent, query_type)
        
        # Call the LegalVision endpoint
        answer = self._call_legalvision_endpoint(prompt)
        
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
        
        # Calculate confidence
        confidence = self._calculate_confidence(graph_data, answer, user_query)
        
        return {
            "answer": answer,
            "reasoning_steps": reasoning_steps,
            "irac_analysis": irac_analysis,
            "sources": self._extract_sources(graph_data),
            "related_statutes": referenced_statutes,
            "confidence": confidence,
            "model": "LegalVision-Llama-3.1-8B"
        }

    def _extract_irac(self, answer: str) -> Optional[Dict[str, str]]:
        """Extract IRAC components from the answer."""
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
        steps = []
        
        # Look for "Step X:" pattern
        step_pattern = r'(?:\*\*)?Step\s*(\d+)(?:\*\*)?[:.]\s*(.+?)(?=(?:\*\*)?Step\s*\d+|\n\n|$)'
        matches = re.findall(step_pattern, answer, re.IGNORECASE | re.DOTALL)
        
        for step_num, text in matches:
            if len(text.strip()) > 10:
                steps.append({
                    "step": int(step_num),
                    "text": text.strip()[:300],
                    "legal_basis": self._extract_legal_basis(text)
                })
        
        return steps if steps else None

    def _extract_legal_basis(self, text: str) -> Optional[str]:
        """Extract legal basis from text."""
        patterns = [
            r'(Section\s+\d+[A-Za-z]?\s+of\s+(?:the\s+)?[\w\s]+(?:Ordinance|Act))',
            r'(Prevention of Frauds Ordinance)',
            r'(Registration of Documents Ordinance)',
            r'(Stamp Duty Act)',
            r'(Prescription Ordinance)',
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
            if record.get('area'):
                sources.add(f"Price Data: {record['area']}")
        
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
        
        # From answer text
        statute_names = [
            'Prevention of Frauds Ordinance',
            'Registration of Documents Ordinance',
            'Stamp Duty Act',
            'Prescription Ordinance',
            'Mortgage Act',
            'Partition Act',
            'Notaries Ordinance',
            'Registration of Title Act',
        ]
        
        answer_lower = answer.lower()
        for name in statute_names:
            if name.lower() in answer_lower:
                statutes.add(name)
        
        return [s for s in statutes if s]

    def _calculate_confidence(self, graph_data: List[Dict], answer: str, query: str) -> float:
        """Calculate confidence score."""
        confidence = 0.5  # Base confidence for fine-tuned model
        
        if graph_data:
            confidence += 0.15
            if len(graph_data) >= 2:
                confidence += 0.1
        
        # Check for statute citations
        if any(word in answer.lower() for word in ['section', 'ordinance', 'act']):
            confidence += 0.1
        
        # Check for structured response
        if 'step' in answer.lower() or 'irac' in answer.lower():
            confidence += 0.1
        
        return min(confidence, 0.95)


# Create singleton instance
llm_service = LLMReasoningService()
