"""
Session Manager
Handles conversation context and session state
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict
import threading


class SessionManager:
    """
    Manages conversation sessions and context for follow-up questions.
    Thread-safe implementation for concurrent requests.
    """
    
    def __init__(self, max_history: int = 10, session_timeout_minutes: int = 30):
        self.sessions: Dict[str, Dict] = {}
        self.max_history = max_history
        self.session_timeout = timedelta(minutes=session_timeout_minutes)
        self._lock = threading.Lock()
    
    def _create_session(self, session_id: str) -> Dict:
        """Create a new session."""
        return {
            "id": session_id,
            "created_at": datetime.now(),
            "last_activity": datetime.now(),
            "context": {
                "deed_code": None,
                "deed_type": None,
                "lot": None,
                "district": None,
                "person": None,
                "statute": None,
                "last_intent": None,
                "last_results": None
            },
            "history": []
        }
    
    def get_session(self, session_id: str) -> Dict:
        """
        Get or create a session.
        
        Args:
            session_id: Unique session identifier
            
        Returns:
            Session data dictionary
        """
        with self._lock:
            # Clean expired sessions periodically
            self._cleanup_expired()
            
            if session_id not in self.sessions:
                self.sessions[session_id] = self._create_session(session_id)
            else:
                self.sessions[session_id]["last_activity"] = datetime.now()
            
            return self.sessions[session_id]
    
    def get_context(self, session_id: str) -> Dict:
        """Get conversation context for a session."""
        session = self.get_session(session_id)
        return session["context"]
    
    def update_context(
        self, 
        session_id: str, 
        intent: str,
        params: Dict,
        results: List[Dict]
    ):
        """
        Update session context based on query results.
        
        Args:
            session_id: Session identifier
            intent: Detected intent
            params: Query parameters
            results: Query results
        """
        with self._lock:
            session = self.get_session(session_id)
            context = session["context"]
            
            # Update from params
            if "code" in params:
                context["deed_code"] = params["code"]
            if "lot" in params and params["lot"] not in ["property?", "property", "this", "that"]:
                context["lot"] = params["lot"]
            if "district" in params:
                context["district"] = params["district"]
            if "name" in params:
                context["person"] = params["name"]
            if "deed_type" in params:
                context["deed_type"] = params["deed_type"]
            
            context["last_intent"] = intent
            
            # Extract from results
            if results:
                record = results[0]
                if record.get("deed_code"):
                    context["deed_code"] = record["deed_code"]
                if record.get("lot"):
                    context["lot"] = record["lot"]
                if record.get("district"):
                    context["district"] = record["district"]
                if record.get("deed_type"):
                    context["deed_type"] = record["deed_type"]
                if record.get("statute_name"):
                    context["statute"] = record["statute_name"]
            
            context["last_results"] = results[:3] if results else None
    
    def add_to_history(
        self,
        session_id: str,
        query: str,
        intent: str,
        params: Dict,
        results_count: int,
        response_summary: str = None
    ):
        """
        Add a query to session history.
        
        Args:
            session_id: Session identifier
            query: User query
            intent: Detected intent
            params: Query parameters
            results_count: Number of results found
            response_summary: Brief summary of response
        """
        with self._lock:
            session = self.get_session(session_id)
            
            session["history"].append({
                "timestamp": datetime.now().isoformat(),
                "query": query,
                "intent": intent,
                "params": params,
                "results_count": results_count,
                "response_summary": response_summary
            })
            
            # Trim history if too long
            if len(session["history"]) > self.max_history:
                session["history"] = session["history"][-self.max_history:]
    
    def get_history(self, session_id: str) -> List[Dict]:
        """Get conversation history for a session."""
        session = self.get_session(session_id)
        return session["history"]
    
    def clear_session(self, session_id: str):
        """Clear a session's context and history."""
        with self._lock:
            if session_id in self.sessions:
                self.sessions[session_id] = self._create_session(session_id)
    
    def delete_session(self, session_id: str):
        """Delete a session entirely."""
        with self._lock:
            if session_id in self.sessions:
                del self.sessions[session_id]
    
    def _cleanup_expired(self):
        """Remove expired sessions."""
        now = datetime.now()
        expired = [
            sid for sid, session in self.sessions.items()
            if now - session["last_activity"] > self.session_timeout
        ]
        for sid in expired:
            del self.sessions[sid]
    
    def get_session_stats(self) -> Dict:
        """Get statistics about active sessions."""
        with self._lock:
            return {
                "active_sessions": len(self.sessions),
                "total_queries": sum(
                    len(s["history"]) for s in self.sessions.values()
                )
            }


# Create singleton instance
session_manager = SessionManager()
