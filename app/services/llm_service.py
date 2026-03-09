"""
LegalVision Multi-Agent LLM Service
====================================

Uses TWO agents:
1. LegalVision Agent (Fine-tuned Llama 3.1) - Legal reasoning, IRAC, statutes
2. GPT-4o Agent - Knowledge graph queries, deed lookups, data extraction
"""

import requests
import re
from typing import Dict, List, Optional, Any
from app.core.config import settings
from app.utils.intent_detection import QueryType


class LLMReasoningService:
    """
    Multi-agent service for legal queries.
    """
    
    def __init__(self):
        # LegalVision endpoint config
        self.legalvision_url = getattr(settings, 'LEGALVISION_ENDPOINT_URL', '') or "https://scva0fjg3t7o0ykh.us-east-1.aws.endpoints.huggingface.cloud"
        self.hf_token = getattr(settings, 'HF_TOKEN', '')
        
        self.legalvision_headers = {"Content-Type": "application/json"}
        if self.hf_token:
            self.legalvision_headers["Authorization"] = f"Bearer {self.hf_token}"
        
        # GPT config - lazy load client
        self._openai_client = None
        self.gpt_model = getattr(settings, 'OPENAI_MODEL', 'gpt-4o')
        
        # Generation parameters
        self.max_tokens = 1024
        self.temperature = 0.7
        
        # System prompts
        self.legalvision_system_prompt = """You are a Sri Lankan property law expert. Provide step-by-step legal reasoning with references to relevant statutes. Structure your response with:
1. A clear answer
2. Step-by-step reasoning
3. IRAC analysis (Issue, Rule, Application, Conclusion)
4. Relevant legal references
5. Practical examples where helpful"""

        self.graph_system_prompt = """You are a legal assistant specialized in Sri Lankan property law.
You help users understand property deeds, ownership transfers, and legal documents.

When answering:
1. Use the provided graph data to give accurate, specific answers
2. Reference deed numbers, party names, and property details
3. Format monetary amounts with proper separators (LKR 1,500,000)
4. When showing boundaries, clearly list North, South, East, West
5. When showing parties, clearly indicate their roles"""

    @property
    def openai_client(self):
        """Lazy load OpenAI client."""
        if self._openai_client is None:
            api_key = getattr(settings, 'OPENAI_API_KEY', '')
            if api_key:
                try:
                    from openai import OpenAI
                    self._openai_client = OpenAI(api_key=api_key)
                except Exception as e:
                    print(f"Failed to init OpenAI: {e}")
                    self._openai_client = False
            else:
                self._openai_client = False
        return self._openai_client if self._openai_client else None

    def _should_use_legalvision(self, query: str, intent: str, query_type: QueryType) -> bool:
        """Determine which agent to use."""
        query_lower = query.lower()
        
        # REASONING keywords → LegalVision
        reasoning_keywords = [
            'what are the requirements', 'legal requirements',
            'how to', 'explain', 'what is', 'define',
            'is it valid', 'is it legal', 'can i', 'can foreigners',
            'stamp duty', 'registration fee', 'how much',
            'difference between', 'compare', 'vs',
            'what happens if', 'consequences', 'penalty',
            'prescription', 'adverse possession', 'bim saviya',
            'irac', 'legal analysis', 'ordinance', 'act', 'section',
            'transfer property', 'gift deed', 'sale deed', 'mortgage deed',
            'requirements for'
        ]
        
        # DATA keywords → GPT-4o
        data_keywords = [
            'find deed', 'show deed', 'deed number', 'deed code',
            'who is the', 'who are the parties', 'parties of',
            'boundaries of', 'boundary of', 'adjacent',
            'lot number', 'plan number',
            'deeds in colombo', 'deeds in gampaha', 'in district',
            'how many deeds', 'count', 'statistics', 'total',
            'recent deeds', 'latest',
            'ownership chain', 'prior deed',
            'details of deed', 'summarize deed'
        ]
        
        # Check for deed code pattern → GPT
        if re.search(r'[A-Z]\s*\d+/\d+', query, re.IGNORECASE):
            return False
        
        # Check data keywords → GPT
        if any(kw in query_lower for kw in data_keywords):
            return False
        
        # Check reasoning keywords → LegalVision
        if any(kw in query_lower for kw in reasoning_keywords):
            return True
        
        # QueryType fallback
        reasoning_types = [QueryType.LEGAL_REASONING, QueryType.COMPLIANCE, QueryType.DEFINITION]
        if hasattr(QueryType, 'COST_CALCULATION'):
            reasoning_types.append(QueryType.COST_CALCULATION)
        
        if query_type in reasoning_types:
            return True
        
        return True  # Default to LegalVision

    def _call_legalvision(self, query: str, graph_context: str = "") -> Optional[str]:
        """Call LegalVision model."""
        user_message = f"{query}\n\nRelevant Information:\n{graph_context}" if graph_context else query
        
        prompt = f"""<|begin_of_text|><|start_header_id|>system<|end_header_id|>

{self.legalvision_system_prompt}<|eot_id|><|start_header_id|>user<|end_header_id|>

{user_message}<|eot_id|><|start_header_id|>assistant<|end_header_id|>

"""
        
        payload = {
            "inputs": prompt,
            "parameters": {
                "max_new_tokens": self.max_tokens,
                "temperature": self.temperature,
                "top_p": 0.9,
                "do_sample": True
            }
        }
        
        try:
            response = requests.post(
                self.legalvision_url,
                headers=self.legalvision_headers,
                json=payload,
                timeout=120
            )
            
            if response.status_code == 200:
                result = response.json()
                if isinstance(result, dict):
                    text = result.get("generated_text", "")
                elif isinstance(result, list) and len(result) > 0:
                    text = result[0].get("generated_text", "")
                else:
                    text = str(result)
                return self._clean_response(text)
            else:
                print(f"LegalVision Error: {response.status_code}")
                return None
        except Exception as e:
            print(f"LegalVision Exception: {e}")
            return None

    def _call_gpt(self, query: str, graph_data: List[Dict]) -> str:
        """Call GPT-4o."""
        if not self.openai_client:
            return "OpenAI API not configured."
        
        context = self._format_graph_data(graph_data)
        
        messages = [
            {"role": "system", "content": self.graph_system_prompt},
            {"role": "user", "content": f"Question: {query}\n\nGraph Data:\n{context}"}
        ]
        
        try:
            response = self.openai_client.chat.completions.create(
                model=self.gpt_model,
                messages=messages,
                temperature=0.3,
                max_tokens=1000
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"GPT Error: {e}")
            return f"Error: {e}"

    def _format_graph_data(self, graph_data: List[Dict]) -> str:
        """Format graph data for GPT."""
        if not graph_data:
            return "No data found."
        
        context = ""
        for i, record in enumerate(graph_data[:10], 1):
            context += f"\nResult {i}:\n"
            for key, value in record.items():
                if value and value != []:
                    if key == "parties" and isinstance(value, list):
                        for party in value:
                            if isinstance(party, dict):
                                context += f"  - {party.get('role', 'Party')}: {party.get('name', 'Unknown')}\n"
                    elif isinstance(value, (int, float)) and 'amount' in key.lower():
                        context += f"  {key}: LKR {value:,.0f}\n"
                    else:
                        context += f"  {key}: {value}\n"
        return context

    def _format_simple_context(self, graph_data: List[Dict]) -> str:
        """Format minimal context for LegalVision."""
        if not graph_data:
            return ""
        
        parts = []
        for record in graph_data[:3]:
            if record.get('deed_type'):
                parts.append(f"Deed Type: {record['deed_type']}")
            if record.get('requirements') and isinstance(record['requirements'], list):
                parts.append(f"Requirements: {', '.join(str(r) for r in record['requirements'][:5])}")
            if record.get('stamp_duty') or record.get('stamp_duty_rule'):
                parts.append(f"Stamp Duty: {record.get('stamp_duty') or record.get('stamp_duty_rule')}")
            if record.get('governing_statutes') and isinstance(record['governing_statutes'], list):
                names = [s.get('name', str(s)) if isinstance(s, dict) else str(s) for s in record['governing_statutes']]
                parts.append(f"Statutes: {', '.join(names)}")
        return '\n'.join(parts)

    def _clean_response(self, text: str) -> str:
        """Clean model output."""
        text = re.sub(r'```python.*?```', '', text, flags=re.DOTALL)
        text = re.sub(r'```sql.*?```', '', text, flags=re.DOTALL)
        text = re.sub(r'```.*?```', '', text, flags=re.DOTALL)
        text = re.sub(r'def \w+\(.*?\):.*?(?=\n\n|\Z)', '', text, flags=re.DOTALL)
        text = re.sub(r'SELECT \* FROM.*', '', text, flags=re.DOTALL)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_irac(self, answer: str) -> Optional[Dict[str, str]]:
        """Extract IRAC components."""
        if not answer:
            return None
            
        irac = {}
        patterns = {
            'issue': r'\*?\*?Issue:?\*?\*?\s*(.+?)(?=\*?\*?Rule:?|\n\n|$)',
            'rule': r'\*?\*?Rule:?\*?\*?\s*(.+?)(?=\*?\*?Application:?|\n\n|$)',
            'application': r'\*?\*?Application:?\*?\*?\s*(.+?)(?=\*?\*?Conclusion:?|\n\n|$)',
            'conclusion': r'\*?\*?Conclusion:?\*?\*?\s*(.+?)(?=\n\n|$)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, answer, re.IGNORECASE | re.DOTALL)
            if match:
                value = match.group(1).strip()[:500]
                if len(value) > 10:
                    irac[key] = value
        
        return irac if len(irac) >= 3 else None

    def _extract_reasoning_steps(self, answer: str) -> Optional[List[Dict]]:
        """Extract reasoning steps."""
        if not answer:
            return None
            
        steps = []
        pattern = r'\*?\*?Step\s*(\d+):?\*?\*?\s*(.+?)(?=\*?\*?Step\s*\d+|\*?\*?IRAC|$)'
        matches = re.findall(pattern, answer, re.IGNORECASE | re.DOTALL)
        
        for step_num, text in matches:
            text = text.strip()[:500]
            if len(text) > 10:
                legal_basis = None
                basis_match = re.search(r'Legal Basis:\s*([^\n]+)', text)
                if basis_match:
                    legal_basis = basis_match.group(1).strip()
                steps.append({"step": int(step_num), "text": text, "legal_basis": legal_basis})
        
        return steps if steps else None

    def _extract_sources(self, graph_data: List[Dict]) -> List[str]:
        """Extract sources."""
        sources = set()
        for record in graph_data:
            if record.get('deed_code'):
                sources.add(f"Deed: {record['deed_code']}")
            if record.get('statute_name'):
                sources.add(f"Statute: {record['statute_name']}")
        return list(sources)

    def _extract_statute_references(self, answer: str, graph_data: List[Dict]) -> List[str]:
        """Extract statutes."""
        statutes = set()
        
        for record in graph_data:
            if record.get('statute_name'):
                statutes.add(record['statute_name'])
        
        statute_names = [
            'Prevention of Frauds Ordinance', 'Registration of Documents Ordinance',
            'Stamp Duty Act', 'Prescription Ordinance', 'Mortgage Act',
            'Partition Act', 'Notaries Ordinance', 'Registration of Title Act',
        ]
        
        if answer:
            for name in statute_names:
                if name.lower() in answer.lower():
                    statutes.add(name)
        
        return [s for s in statutes if s]

    def generate_response(
        self,
        user_query: str,
        graph_data: List[Dict],
        intent: str,
        query_type: QueryType,
        conversation_history: List[Dict] = None,
        include_reasoning: bool = True
    ) -> Dict[str, Any]:
        """Generate response using appropriate agent."""
        
        use_legalvision = self._should_use_legalvision(user_query, intent, query_type)
        
        if use_legalvision:
            agent_used = "LegalVision-Llama-3.1-8B"
            graph_context = self._format_simple_context(graph_data)
            answer = self._call_legalvision(user_query, graph_context)
            
            # Fallback to GPT
            if not answer and self.openai_client:
                answer = self._call_gpt(user_query, graph_data)
                agent_used = "GPT-4o (fallback)"
            
            irac_analysis = self._extract_irac(answer)
            reasoning_steps = self._extract_reasoning_steps(answer) if include_reasoning else None
        else:
            agent_used = "GPT-4o"
            answer = self._call_gpt(user_query, graph_data)
            irac_analysis = None
            reasoning_steps = None
        
        confidence = 0.7 if graph_data else 0.6
        if answer and ('Step' in answer or 'IRAC' in answer):
            confidence += 0.1
        
        return {
            "answer": answer or "Unable to generate response.",
            "reasoning_steps": reasoning_steps,
            "irac_analysis": irac_analysis,
            "sources": self._extract_sources(graph_data),
            "related_statutes": self._extract_statute_references(answer or "", graph_data),
            "confidence": min(confidence, 0.95),
            "model": agent_used
        }


# Create singleton instance
llm_service = LLMReasoningService()
