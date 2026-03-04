"""
Database Connection Module
Handles Neo4j connection and session management
"""

from neo4j import GraphDatabase
from typing import List, Dict, Any, Optional
from app.core.config import settings


class Neo4jDriver:
    """Neo4j database driver wrapper."""
    
    def __init__(self):
        self._driver = None
    
    @property
    def driver(self):
        """Lazy initialization of Neo4j driver."""
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                settings.NEO4J_URI,
                auth=(settings.NEO4J_USER, settings.NEO4J_PASS)
            )
        return self._driver
    
    def close(self):
        """Close the driver connection."""
        if self._driver:
            self._driver.close()
            self._driver = None
    
    def session(self):
        """Get a new session."""
        return self.driver.session()
    
    def execute_query(self, cypher: str, params: Dict[str, Any] = None) -> List[Dict]:
        """
        Execute a Cypher query and return results as list of dicts.
        
        Args:
            cypher: Cypher query string
            params: Query parameters
            
        Returns:
            List of result records as dictionaries
        """
        params = params or {}
        try:
            with self.session() as session:
                result = session.run(cypher, params)
                return [dict(record) for record in result]
        except Exception as e:
            print(f"Query error: {e}")
            return []
    
    def execute_single(self, cypher: str, params: Dict[str, Any] = None) -> Optional[Dict]:
        """
        Execute a query expecting a single result.
        
        Args:
            cypher: Cypher query string
            params: Query parameters
            
        Returns:
            Single result record or None
        """
        results = self.execute_query(cypher, params)
        return results[0] if results else None


# Global driver instance
neo4j_driver = Neo4jDriver()
