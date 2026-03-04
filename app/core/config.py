"""
Application Configuration
Loads settings from environment variables
"""

import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """Application settings loaded from environment."""
    
    # API Settings
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DEBUG: bool = True
    
    # Neo4j Settings
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASS: str = os.getenv("NEO4J_PASS", "")
    
    # OpenAI Settings
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    OPENAI_MODEL: str = "gpt-4o"
    OPENAI_TEMPERATURE: float = 0.3
    OPENAI_MAX_TOKENS: int = 1500
    
    # Application Settings
    MAX_RESULTS: int = 10
    CONVERSATION_HISTORY_SIZE: int = 10
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
