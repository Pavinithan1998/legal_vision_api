# LegalVision API

**Sri Lankan Property Law GraphRAG System**

A comprehensive FastAPI-based REST API for querying Sri Lankan property law knowledge graph with legal reasoning capabilities.

## Features

- 🔍 **Natural Language Queries**: Ask questions in plain English
- 📜 **Deed Lookup**: Search and retrieve deed information
- ⚖️ **Legal Reasoning**: Get IRAC-formatted legal analysis
- ✅ **Compliance Checking**: Validate deed compliance with Sri Lankan law
- 📚 **Statute Lookup**: Search Sri Lankan property law statutes
- 📖 **Definition Lookup**: Get legal term definitions
- 💬 **Conversation Context**: Follow-up questions supported

## Project Structure

```
legalvision_api/
├── main.py                     # FastAPI application entry point
├── requirements.txt            # Python dependencies
├── .env.example               # Environment variables template
├── app/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py          # Application configuration
│   │   └── database.py        # Neo4j connection
│   ├── models/
│   │   ├── __init__.py
│   │   └── schemas.py         # Pydantic models
│   ├── routers/
│   │   ├── __init__.py
│   │   ├── query.py           # Natural language query endpoint
│   │   ├── deeds.py           # Deed-specific endpoints
│   │   ├── legal.py           # Statute/principle endpoints
│   │   ├── definitions.py     # Legal definitions endpoints
│   │   └── compliance.py      # Compliance checking endpoints
│   ├── services/
│   │   ├── __init__.py
│   │   ├── graph_service.py   # Neo4j operations
│   │   ├── llm_service.py     # GPT-4o reasoning
│   │   └── session_manager.py # Conversation context
│   └── utils/
│       ├── __init__.py
│       ├── cypher_queries.py  # All Cypher queries
│       └── intent_detection.py # NLP intent detection
├── data/                       # Data files (optional)
└── tests/                      # Test files
```

## Installation

1. **Clone and setup:**
```bash
cd legalvision_api
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. **Configure environment:**
```bash
cp .env.example .env
# Edit .env with your credentials
```

3. **Start the server:**
```bash
# Development
uvicorn main:app --reload

# Production
uvicorn main:app --host 0.0.0.0 --port 8000
```

4. **Access the API:**
- API: http://localhost:8000
- Docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## API Endpoints

### Query Endpoints (`/api/v1`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/query` | Natural language query with reasoning |
| GET | `/stats` | Knowledge graph statistics |
| GET | `/search?q=` | General search across all entities |

### Deed Endpoints (`/api/v1/deeds`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/{deed_code}` | Get full deed details |
| GET | `/{deed_code}/parties` | Get deed parties |
| GET | `/{deed_code}/boundaries` | Get property boundaries |
| GET | `/{deed_code}/history` | Get ownership chain |
| GET | `/{deed_code}/governing-law` | Get applicable statutes |
| GET | `/by-person/{name}` | Search by person name |
| GET | `/by-district/{district}` | Search by district |
| GET | `/by-type/{type}` | Search by deed type |
| GET | `/recent/` | Get recent deeds |

### Legal Endpoints (`/api/v1/legal`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/statutes` | List all statutes |
| GET | `/statutes/search?q=` | Search statutes |
| GET | `/statutes/for-deed-type/{type}` | Statutes for deed type |
| GET | `/principles` | List legal principles |
| GET | `/requirements/{deed_type}` | Get deed requirements |
| POST | `/reason` | Legal reasoning with IRAC |
| GET | `/explain/{term}` | Explain legal concept |

### Definition Endpoints (`/api/v1/definitions`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | List all definitions |
| GET | `/search?q=` | Search definitions |
| GET | `/term/{term}` | Get specific definition |

### Compliance Endpoints (`/api/v1/compliance`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/check` | Check deed compliance |
| GET | `/check/{deed_code}` | Check by deed code |
| GET | `/requirements/{type}` | Get compliance requirements |
| POST | `/analyze/{deed_code}` | Detailed compliance analysis |
| GET | `/validate/{type}` | Get validation checklist |

## Example Usage

### Natural Language Query
```bash
curl -X POST "http://localhost:8000/api/v1/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "What laws govern sale deeds in Sri Lanka?", "include_reasoning": true}'
```

### Get Deed Details
```bash
curl "http://localhost:8000/api/v1/deeds/A%201100%2F188"
```

### Check Compliance
```bash
curl -X POST "http://localhost:8000/api/v1/compliance/check" \
  -H "Content-Type: application/json" \
  -d '{"deed_code": "A 1100/188"}'
```

### Search Statutes
```bash
curl "http://localhost:8000/api/v1/legal/statutes/search?q=Prevention%20of%20Frauds"
```

## Response Format

### Query Response
```json
{
  "query": "What laws govern sale deeds?",
  "intent": "find_statutes_for_deed_type",
  "query_type": "legal_reasoning",
  "answer": "Sale deeds in Sri Lanka are governed by...",
  "irac_analysis": {
    "issue": "What statutes govern sale deeds?",
    "rule": "Prevention of Frauds Ordinance...",
    "application": "For a sale deed to be valid...",
    "conclusion": "Sale deeds must comply with..."
  },
  "reasoning_steps": [...],
  "related_statutes": ["Prevention of Frauds Ordinance", ...],
  "confidence": 0.85
}
```

## Integration with Sivanuja's Reasoning Module

This API is designed to integrate with the Explainable Legal Reasoning Module:

1. **Context Provider**: The GraphRAG provides factual context from the knowledge graph
2. **Reasoning Trigger**: Legal queries can trigger the fine-tuned LLM for deeper reasoning
3. **Data Flow**:
   ```
   User Query → GraphRAG (facts) → Reasoning Module (analysis) → Combined Response
   ```

## Development

### Running Tests
```bash
pytest tests/ -v
```

### Adding New Queries
1. Add Cypher query to `app/utils/cypher_queries.py`
2. Add intent to `app/utils/intent_detection.py`
3. Add service method to `app/services/graph_service.py`
4. Add endpoint to appropriate router

## License

Part of LegalVision Project - 25-26J-127

## Contributors

- S. Sharan - Knowledge Graph Component
- S. Sivanuja - Reasoning Module
