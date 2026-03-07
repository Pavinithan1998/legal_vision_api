"""
LegalVision API - Sri Lankan Property Law GraphRAG System
Main FastAPI Application Entry Point
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import traceback
import logging

from app.core.config import settings
from app.core.database import neo4j_driver
from app.routers import query, deeds, legal, definitions, compliance

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - startup and shutdown."""
    # Startup
    print("🚀 Starting LegalVision API...")
    print(f"📊 Connecting to Neo4j: {settings.NEO4J_URI}")
    yield
    # Shutdown
    print("👋 Shutting down LegalVision API...")
    neo4j_driver.close()


app = FastAPI(
    title="LegalVision API",
    description="""
    ## Sri Lankan Property Law GraphRAG System
    
    A comprehensive API for querying Sri Lankan property law knowledge graph
    with legal reasoning capabilities.
    
    ### Features:
    - **Natural Language Queries**: Ask questions in plain English
    - **Deed Lookup**: Search and retrieve deed information
    - **Legal Reasoning**: Get IRAC-formatted legal analysis
    - **Compliance Checking**: Validate deed compliance with Sri Lankan law
    - **Statute Lookup**: Search Sri Lankan property law statutes
    - **Definition Lookup**: Get legal term definitions
    
    ### Authentication:
    Currently no authentication required for development.
    """,
    version="2.0.0",
    lifespan=lifespan
)


# Add global exception handler
@app.middleware("http")
async def error_handler_middleware(request: Request, call_next):
    """Catch all exceptions and log them."""
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"Unhandled exception in {request.url.path}: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={
                "detail": f"Internal server error: {str(e)}",
                "path": request.url.path
            }
        )


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(query.router, prefix="/api/v1", tags=["Query"])
app.include_router(deeds.router, prefix="/api/v1/deeds", tags=["Deeds"])
app.include_router(legal.router, prefix="/api/v1/legal", tags=["Legal"])
app.include_router(definitions.router, prefix="/api/v1/definitions", tags=["Definitions"])
app.include_router(compliance.router, prefix="/api/v1/compliance", tags=["Compliance"])


@app.get("/", tags=["Health"])
async def root():
    """Root endpoint - API information."""
    return {
        "name": "LegalVision API",
        "version": "2.0.0",
        "description": "Sri Lankan Property Law GraphRAG System",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    try:
        # Test Neo4j connection
        with neo4j_driver.session() as session:
            result = session.run("RETURN 1 AS test")
            result.single()
        neo4j_status = "connected"
    except Exception as e:
        neo4j_status = f"error: {str(e)}"
    
    return {
        "status": "healthy" if neo4j_status == "connected" else "degraded",
        "neo4j": neo4j_status,
        "openai": "configured" if settings.OPENAI_API_KEY else "not configured"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
