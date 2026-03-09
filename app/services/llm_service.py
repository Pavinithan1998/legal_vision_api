"""
LegalVision Multi-Agent LLM Service (V4 - Stronger Initial Answers)
====================================================================

Key Improvement: Forces COMPREHENSIVE answers from the FIRST response
- No more surface-level initial answers
- Practical details included by default
- Real costs, timeframes, and examples

Agents:
- LegalVision (Fine-tuned Llama 3.1) - Legal reasoning, IRAC, statutes
- GPT-4o - Knowledge graph queries, deed lookups, data extraction
"""

import requests
import re
from typing import Dict, List, Optional, Any
from app.core.config import settings
from app.utils.intent_detection import QueryType


class LLMReasoningService:
    """
    Enhanced multi-agent service with stronger initial answers.
    """
    
    def __init__(self):
        # =====================================================
        # AGENT 1: LegalVision (Fine-tuned Llama 3.1)
        # =====================================================
        self.legalvision_url = getattr(settings, 'LEGALVISION_ENDPOINT_URL', '') or "https://scva0fjg3t7o0ykh.us-east-1.aws.endpoints.huggingface.cloud"
        self.hf_token = getattr(settings, 'HF_TOKEN', '')
        
        self.legalvision_headers = {"Content-Type": "application/json"}
        if self.hf_token:
            self.legalvision_headers["Authorization"] = f"Bearer {self.hf_token}"
        
        # =====================================================
        # AGENT 2: GPT-4o (Lazy loaded)
        # =====================================================
        self._openai_client = None
        self.gpt_model = getattr(settings, 'OPENAI_MODEL', 'gpt-4o')
        
        # Generation parameters
        self.max_tokens = 2048
        self.temperature = 0.7
        
        # =====================================================
        # STRONGER SYSTEM PROMPT - Forces comprehensive answers
        # =====================================================
        self.legalvision_system_prompt = """You are an EXPERT Sri Lankan property law attorney with 25+ years of experience. You provide COMPREHENSIVE, PRACTICAL legal advice that clients can actually USE.

CRITICAL: Your FIRST answer must be COMPLETE and DETAILED. Do NOT give surface-level answers expecting follow-up questions.

YOUR RESPONSE MUST INCLUDE ALL THESE SECTIONS:

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Answer:** 
[2-3 sentence direct answer]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Detailed Explanation:**

**What is Required:**
[Explain each requirement clearly]

**Why These Requirements Exist:**
[Explain the PURPOSE behind each law - fraud prevention, public notice, etc.]

**What Happens if Requirements Are Not Met:**
[Explain consequences - deed invalid, penalties, disputes, etc.]

**Costs Involved:**
- Stamp Duty: [Exact percentage, e.g., "3% for properties up to LKR 100 million, 4% above"]
- Registration Fee: [Exact amount or percentage]
- Notary Fees: [Typical range]
- Total Estimated Cost: [For a typical transaction]

**Timeframes:**
- Deed Preparation: [X days/weeks]
- Execution: [X days]
- Registration: [X weeks]
- Total Process: [X weeks/months]

**Documents Required:**
1. [Document 1]
2. [Document 2]
3. [Continue listing all required documents]

**Government Offices Involved:**
- [Office 1]: [What they do]
- [Office 2]: [What they do]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Step-by-Step Process:**

**Step 1: [Title]**
- What to do: [Detailed instructions]
- Legal Basis: [Statute, Section X]
- Documents needed: [List]
- Time required: [Duration]
- Cost: [If applicable]

**Step 2: [Title]**
[Continue with same detail for each step - minimum 6 steps]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**IRAC Analysis:**

- **Issue:** [State the legal question precisely]

- **Rule:** [Cite ALL applicable statutes with SPECIFIC SECTION NUMBERS]
  • Prevention of Frauds Ordinance, Section 2: [What it says]
  • Registration of Documents Ordinance, Section 7: [What it says]
  • [Continue for all relevant statutes]

- **Application:** [Apply each rule to the situation - be thorough, 3-4 sentences minimum]

- **Conclusion:** [Clear conclusion with practical advice]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Legal References:**
- [Statute Name], Section [X]: [Brief description of what it covers]
- [Continue for all statutes - minimum 4 references]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Practical Example:**

*Scenario:* Mr. Perera owns a house in Colombo worth LKR 25 million. He wants to sell it to Mrs. Silva.

*Step-by-step what they must do:*
1. [First step with specific details]
2. [Second step]
3. [Continue...]

*Costs for this transaction:*
- Stamp Duty: LKR [amount] (3% of LKR 25 million)
- Registration: LKR [amount]
- Notary: LKR [amount]
- Total: LKR [amount]

*Timeline:* [How long this specific transaction would take]

*What could go wrong:*
- [Potential issue 1 and how to avoid]
- [Potential issue 2 and how to avoid]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Common Mistakes to Avoid:**
1. [Mistake 1]: [Why it's a problem and how to avoid]
2. [Mistake 2]: [Why it's a problem and how to avoid]
3. [Continue - minimum 5 mistakes]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
**Practical Tips from a Lawyer:**
1. [Tip 1 - insider advice]
2. [Tip 2]
3. [Continue - minimum 5 tips]

CRITICAL RULES:
1. ALWAYS give COMPLETE answers - never expect follow-up questions
2. ALWAYS include REAL COSTS with actual numbers/percentages
3. ALWAYS include TIMEFRAMES
4. ALWAYS include PRACTICAL EXAMPLES with specific numbers
5. ALWAYS explain WHY each requirement exists
6. ALWAYS cite SPECIFIC SECTION NUMBERS
7. ALWAYS include COMMON MISTAKES and how to avoid them
8. Write as if the client will use this to actually DO the transaction
9. Use SIMPLE LANGUAGE - explain legal terms when used
10. Be THOROUGH - better to over-explain than under-explain"""

        self.graph_system_prompt = """You are a legal data assistant specialized in Sri Lankan property law.

When answering from graph data:
1. Present data clearly and organized
2. Explain the SIGNIFICANCE of each piece of data
3. Format monetary amounts properly (LKR 1,500,000)
4. List boundaries as: North, South, East, West
5. Clearly indicate party roles (Vendor, Vendee, Notary, Witness)
6. If data is incomplete, state what's missing
7. Provide context about what the information means legally"""

        # =====================================================
        # FOLLOW-UP PROMPT - Even deeper explanations
        # =====================================================
        self.followup_enhancement_prompt = """
The user wants MORE DETAILS about the previous topic.

Provide DEEPER and MORE COMPREHENSIVE information:

1. **Advanced Legal Nuances:**
   - Exceptions to the general rules
   - Special cases and edge situations
   - Recent legal developments or amendments
   - Relevant case law examples

2. **Detailed Process Breakdown:**
   - Sub-steps within each main step
   - Who to contact and when
   - What forms to fill
   - Where to submit documents

3. **Financial Deep Dive:**
   - Breakdown of all costs
   - Payment methods accepted
   - Penalties for late payment
   - Ways to reduce costs legally

4. **Risk Analysis:**
   - What can go wrong at each step
   - How to verify everything is correct
   - Red flags to watch for
   - When to consult a lawyer

5. **Practical Checklist:**
   - Before starting the process
   - During the process
   - After completion

DO NOT repeat the basic information - go DEEPER into specifics.
"""

    # =========================================================================
    # LAZY LOADING
    # =========================================================================
    
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

    # =========================================================================
    # CONVERSATION CONTEXT
    # =========================================================================
    
    def _build_conversation_context(self, conversation_history: List[Dict]) -> str:
        """Build context from conversation history."""
        if not conversation_history:
            return ""
        
        context = "\n══════════ PREVIOUS CONVERSATION ══════════\n"
        
        for i, exchange in enumerate(conversation_history[-5:], 1):
            if exchange.get('query'):
                context += f"\n[Question {i}]: {exchange['query']}\n"
            if exchange.get('answer'):
                # Include key parts of answer
                answer = exchange['answer']
                if len(answer) > 1000:
                    # Extract main answer section
                    if "**Answer:**" in answer:
                        match = re.search(r'\*\*Answer:\*\*\s*(.+?)(?=\*\*Detailed|\n\n\*\*|$)', answer, re.DOTALL)
                        if match:
                            context += f"[Answer {i}]: {match.group(1).strip()[:500]}...\n"
                    else:
                        context += f"[Answer {i}]: {answer[:500]}...\n"
                else:
                    context += f"[Answer {i}]: {answer}\n"
        
        context += "\n══════════ END PREVIOUS CONVERSATION ══════════\n"
        return context
    
    def _is_follow_up_question(self, query: str) -> bool:
        """Detect follow-up questions."""
        follow_up_words = [
            'explain', 'more', 'detail', 'elaborate', 'clarify', 'deeper',
            'what do you mean', 'tell me more', 'expand', 'further',
            'what about', 'how about', 'and also', 'continue',
            'previous', 'above', 'that', 'this', 'it', 'these',
            'why', 'how come', 'example', 'specifically'
        ]
        
        query_lower = query.lower().strip()
        
        # Short queries are likely follow-ups
        if len(query_lower.split()) < 7:
            return True
        
        return any(word in query_lower for word in follow_up_words)
    
    def _enhance_query_with_context(self, query: str, conversation_history: List[Dict]) -> str:
        """Add context to vague follow-up queries."""
        if not conversation_history:
            return query
        
        # Get last topic
        last_query = None
        for exchange in reversed(conversation_history):
            if exchange.get('query'):
                last_query = exchange['query']
                break
        
        if last_query and len(query.split()) < 8:
            return f"Regarding '{last_query}': {query}"
        
        return query

    # =========================================================================
    # AGENT ROUTING
    # =========================================================================
    
    def _should_use_legalvision(self, query: str, intent: str, query_type: QueryType) -> bool:
        """Determine which agent to use."""
        query_lower = query.lower()
        
        # Data queries → GPT-4o
        data_keywords = [
            'find deed', 'show deed', 'deed number', 'deed code',
            'who is the', 'who are the parties', 'parties of',
            'boundaries of', 'boundary', 'adjacent', 'neighbor',
            'lot number', 'plan number', 'in district',
            'how many deeds', 'count', 'statistics', 'total',
            'recent deeds', 'latest', 'ownership chain', 'prior deed'
        ]
        
        # Check for deed code pattern
        if re.search(r'[A-Z]\s*\d+/\d+', query, re.IGNORECASE):
            return False
        
        if any(kw in query_lower for kw in data_keywords):
            return False
        
        # Everything else → LegalVision
        return True

    # =========================================================================
    # AGENT 1: LegalVision
    # =========================================================================
    
    def _call_legalvision(
        self, 
        query: str, 
        graph_context: str = "", 
        conversation_history: List[Dict] = None,
        is_follow_up: bool = False
    ) -> Optional[str]:
        """Call LegalVision with enhanced prompts."""
        
        # Build conversation context
        history_context = ""
        if conversation_history:
            history_context = self._build_conversation_context(conversation_history)
        
        # Enhance vague queries
        if is_follow_up and conversation_history:
            query = self._enhance_query_with_context(query, conversation_history)
        
        # Build user message
        user_parts = []
        
        if history_context:
            user_parts.append(history_context)
        
        user_parts.append(f"\n**QUESTION:** {query}\n")
        
        if graph_context:
            user_parts.append(f"\n**RELEVANT DATA:**\n{graph_context}\n")
        
        # Add enhancement prompt for follow-ups
        if is_follow_up:
            user_parts.append(self.followup_enhancement_prompt)
        else:
            # Force comprehensive initial answer
            user_parts.append("""
REMEMBER: Provide a COMPLETE, COMPREHENSIVE answer. Include:
✓ Real costs with actual numbers
✓ Actual timeframes
✓ Specific section numbers for all statutes
✓ Practical example with specific scenario
✓ Common mistakes to avoid
✓ Step-by-step process

Do NOT give a surface-level answer expecting follow-up questions.
""")
        
        user_message = "\n".join(user_parts)
        
        # Build prompt
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
                timeout=150
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

    # =========================================================================
    # AGENT 2: GPT-4o
    # =========================================================================
    
    def _call_gpt(
        self, 
        query: str, 
        graph_data: List[Dict],
        conversation_history: List[Dict] = None
    ) -> str:
        """Call GPT-4o for data queries."""
        if not self.openai_client:
            return "OpenAI API not configured."
        
        context = self._format_graph_data(graph_data)
        
        messages = [{"role": "system", "content": self.graph_system_prompt}]
        
        # Add history
        if conversation_history:
            for exchange in conversation_history[-3:]:
                if exchange.get('query'):
                    messages.append({"role": "user", "content": exchange['query']})
                if exchange.get('answer'):
                    messages.append({"role": "assistant", "content": exchange['answer'][:600]})
        
        messages.append({
            "role": "user", 
            "content": f"Question: {query}\n\nData:\n{context}"
        })
        
        try:
            response = self.openai_client.chat.completions.create(
                model=self.gpt_model,
                messages=messages,
                temperature=0.3,
                max_tokens=1500
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Error: {e}"

    # =========================================================================
    # FORMATTING
    # =========================================================================
    
    def _format_graph_data(self, graph_data: List[Dict]) -> str:
        """Format graph data."""
        if not graph_data:
            return "No data found."
        
        context = ""
        for i, record in enumerate(graph_data[:10], 1):
            context += f"\nRecord {i}:\n"
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
        """Format minimal context."""
        if not graph_data:
            return ""
        
        parts = []
        for record in graph_data[:5]:
            if record.get('deed_type'):
                parts.append(f"Deed Type: {record['deed_type']}")
            if record.get('requirements'):
                reqs = record['requirements']
                if isinstance(reqs, list):
                    parts.append(f"Requirements: {', '.join(str(r) for r in reqs[:5])}")
            if record.get('stamp_duty') or record.get('stamp_duty_rule'):
                parts.append(f"Stamp Duty: {record.get('stamp_duty') or record.get('stamp_duty_rule')}")
            if record.get('statute_name'):
                parts.append(f"Statute: {record['statute_name']}")
        return '\n'.join(parts)

    def _clean_response(self, text: str) -> str:
        """Clean response."""
        if not text:
            return ""
        
        # Remove code blocks
        text = re.sub(r'```.*?```', '', text, flags=re.DOTALL)
        text = re.sub(r'def \w+\(.*?\):.*?(?=\n\n|\Z)', '', text, flags=re.DOTALL)
        text = re.sub(r'SELECT \* FROM.*', '', text, flags=re.DOTALL)
        text = re.sub(r'\n{4,}', '\n\n\n', text)
        
        return text.strip()

    # =========================================================================
    # EXTRACTION METHODS
    # =========================================================================
    
    def _extract_irac(self, answer: str) -> Optional[Dict[str, str]]:
        """Extract IRAC."""
        if not answer:
            return None
        
        irac = {}
        patterns = {
            'issue': r'\*\*Issue:\*\*\s*(.+?)(?=\*\*Rule:|\n\n\*\*|$)',
            'rule': r'\*\*Rule:\*\*\s*(.+?)(?=\*\*Application:|\n\n\*\*|$)',
            'application': r'\*\*Application:\*\*\s*(.+?)(?=\*\*Conclusion:|\n\n\*\*|$)',
            'conclusion': r'\*\*Conclusion:\*\*\s*(.+?)(?=\n\n\*\*|$)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, answer, re.IGNORECASE | re.DOTALL)
            if match:
                value = match.group(1).strip()
                if len(value) > 15:
                    irac[key] = value[:800]
        
        return irac if len(irac) >= 3 else None

    def _extract_reasoning_steps(self, answer: str) -> Optional[List[Dict]]:
        """Extract reasoning steps."""
        if not answer:
            return None
        
        steps = []
        pattern = r'\*\*Step\s*(\d+)[:\*]*\s*(.+?)(?=\*\*Step\s*\d+|\*\*IRAC|\*\*Legal Ref|\*\*Practical|$)'
        matches = re.findall(pattern, answer, re.IGNORECASE | re.DOTALL)
        
        for step_num, text in matches:
            text = text.strip()
            if len(text) > 15:
                legal_basis = None
                basis_match = re.search(r'Legal Basis:\s*([^\n]+)', text, re.IGNORECASE)
                if basis_match:
                    legal_basis = basis_match.group(1).strip()
                
                result = None
                result_match = re.search(r'Result:\s*([^\n]+)', text, re.IGNORECASE)
                if result_match:
                    result = result_match.group(1).strip()
                
                steps.append({
                    "step": int(step_num),
                    "text": text[:700],
                    "legal_basis": legal_basis,
                    "result": result
                })
        
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
        
        known_statutes = [
            'Prevention of Frauds Ordinance', 'Registration of Documents Ordinance',
            'Stamp Duty Act', 'Prescription Ordinance', 'Mortgage Act',
            'Partition Act', 'Notaries Ordinance', 'Registration of Title Act',
            'Land Development Ordinance', 'Land (Restrictions on Alienation) Act'
        ]
        
        if answer:
            for statute in known_statutes:
                if statute.lower() in answer.lower():
                    statutes.add(statute)
        
        return [s for s in statutes if s]

    def _calculate_confidence(self, graph_data: List[Dict], answer: str, agent: str) -> float:
        """Calculate confidence."""
        if not answer:
            return 0.3
        
        confidence = 0.60
        
        if graph_data:
            confidence += 0.05
        
        # Quality indicators
        indicators = ['**Answer:**', '**Step', 'IRAC', 'Legal Basis:', 'LKR', 'Section']
        for ind in indicators:
            if ind in answer:
                confidence += 0.03
        
        if len(answer) > 2000:
            confidence += 0.05
        
        return min(confidence, 0.95)

    # =========================================================================
    # MAIN METHOD
    # =========================================================================
    
    def generate_response(
        self,
        user_query: str,
        graph_data: List[Dict],
        intent: str,
        query_type: QueryType,
        conversation_history: List[Dict] = None,
        include_reasoning: bool = True
    ) -> Dict[str, Any]:
        """Generate response with comprehensive initial answers."""
        
        use_legalvision = self._should_use_legalvision(user_query, intent, query_type)
        is_follow_up = self._is_follow_up_question(user_query)
        
        if use_legalvision:
            agent_used = "LegalVision-Llama-3.1-8B"
            graph_context = self._format_simple_context(graph_data)
            
            answer = self._call_legalvision(
                query=user_query,
                graph_context=graph_context,
                conversation_history=conversation_history,
                is_follow_up=is_follow_up
            )
            
            # Fallback
            if not answer and self.openai_client:
                answer = self._call_gpt(user_query, graph_data, conversation_history)
                agent_used = "GPT-4o (fallback)"
            
            irac_analysis = self._extract_irac(answer)
            reasoning_steps = self._extract_reasoning_steps(answer) if include_reasoning else None
        else:
            agent_used = "GPT-4o"
            answer = self._call_gpt(user_query, graph_data, conversation_history)
            irac_analysis = None
            reasoning_steps = None
        
        return {
            "answer": answer or "Unable to generate response.",
            "reasoning_steps": reasoning_steps,
            "irac_analysis": irac_analysis,
            "sources": self._extract_sources(graph_data),
            "related_statutes": self._extract_statute_references(answer or "", graph_data),
            "confidence": self._calculate_confidence(graph_data, answer or "", agent_used),
            "model": agent_used
        }


# Singleton
llm_service = LLMReasoningService()
