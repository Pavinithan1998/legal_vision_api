"""
Neo4j Loader for LegalVision Knowledge Graph
Updated for ImprovedHybridDeedExtractor v3 JSON structure
"""

import os
import json
import pathlib
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

load_dotenv()

from neo4j import GraphDatabase

# Configuration
JSON_DIR = pathlib.Path("./deeds/processed_final")  
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")


class LegalKnowledgeGraphLoader:
    """
    Loads extracted deed data into Neo4j knowledge graph.
    Compatible with ImprovedHybridDeedExtractor v3 output structure.
    """

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.stats = {
            "instruments": 0,
            "persons": 0,
            "parcels": 0,
            "plans": 0,
            "errors": 0
        }

    def close(self):
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # =========================================================================
    # CORE INSTRUMENT NODE
    # =========================================================================
    def merge_instrument(self, tx, d: Dict):
        """
        Create/update the main Instrument node with all properties.
        New fields: province, prior_deed, consideration_lkr, source metadata
        """
        query = """
        MERGE (i:Instrument {code_number: $code})
        ON CREATE SET 
            i.id = $id,
            i.type = $type,
            i.date = $date,
            i.consideration_lkr = $consideration,
            i.prior_deed = $prior_deed,
            i.extraction_method = $extraction_method,
            i.quality_score = $quality_score,
            i.quality_rating = $quality_rating,
            i.needs_review = $needs_review,
            i.created_at = datetime()
        ON MATCH SET
            i.type = $type,
            i.date = $date,
            i.consideration_lkr = $consideration,
            i.prior_deed = $prior_deed,
            i.extraction_method = $extraction_method,
            i.quality_score = $quality_score,
            i.quality_rating = $quality_rating,
            i.needs_review = $needs_review,
            i.updated_at = datetime()

        // Registry Office
        FOREACH (_ IN CASE WHEN $registry IS NOT NULL THEN [1] ELSE [] END |
            MERGE (r:RegistryOffice {name: $registry})
            MERGE (i)-[:REGISTERED_AT]->(r)
        )

        // Jurisdiction (District)
        FOREACH (_ IN CASE WHEN $jurisdiction IS NOT NULL THEN [1] ELSE [] END |
            MERGE (j:Jurisdiction {name: $jurisdiction})
            MERGE (i)-[:UNDER_JURISDICTION]->(j)
        )

        // District Node (separate from jurisdiction for clarity)
        FOREACH (_ IN CASE WHEN $district IS NOT NULL THEN [1] ELSE [] END |
            MERGE (d:District {name: $district})
            MERGE (i)-[:IN_DISTRICT]->(d)
        )

        // Province Node (NEW)
        FOREACH (_ IN CASE WHEN $province IS NOT NULL THEN [1] ELSE [] END |
            MERGE (pv:Province {name: $province})
            MERGE (i)-[:IN_PROVINCE]->(pv)
            // Link district to province if both exist
            FOREACH (__ IN CASE WHEN $district IS NOT NULL THEN [1] ELSE [] END |
                MERGE (dd:District {name: $district})
                MERGE (dd)-[:LOCATED_IN]->(pv)
            )
        )

        RETURN i.code_number AS code
        """

        # Extract source metadata
        source = d.get("source", {})
        quality = source.get("quality_score", {})

        tx.run(
            query,
            code=d["code_number"],
            id=d.get("id"),
            type=d.get("type"),
            date=d.get("date"),
            consideration=d.get("consideration_lkr"),
            prior_deed=d.get("prior_deed"),
            registry=d.get("registry_office"),
            jurisdiction=d.get("jurisdiction"),
            district=d.get("district"),
            province=d.get("province"),
            extraction_method=source.get("extraction_method"),
            quality_score=quality.get("percentage"),
            quality_rating=quality.get("rating"),
            needs_review=quality.get("needs_review", False)
        )
        self.stats["instruments"] += 1

    # =========================================================================
    # PLAN AND PROPERTY PARCEL
    # =========================================================================
    def merge_plan_and_parcel(self, tx, d: Dict):
        """
        Create Plan and PropertyParcel nodes with relationships.
        Updated structure: plan and property are separate objects
        """
        plan = d.get("plan", {}) or {}
        prop = d.get("property", {}) or {}

        plan_no = plan.get("plan_no")
        plan_date = plan.get("plan_date")
        surveyor = plan.get("surveyor")

        lot = prop.get("lot")
        extent = prop.get("extent")
        assessment_no = prop.get("assessment_no")
        boundaries = prop.get("boundaries", {}) or {}

        query = """
        MATCH (i:Instrument {code_number: $code})

        // Create Plan if plan_no exists
        FOREACH (_ IN CASE WHEN $plan_no IS NOT NULL THEN [1] ELSE [] END |
            MERGE (pl:Plan {plan_no: $plan_no})
            ON CREATE SET 
                pl.plan_date = $plan_date,
                pl.surveyor = $surveyor
            ON MATCH SET
                pl.plan_date = COALESCE($plan_date, pl.plan_date),
                pl.surveyor = COALESCE($surveyor, pl.surveyor)
            MERGE (i)-[:REFERENCES_PLAN]->(pl)
        )

        // Create PropertyParcel - use lot+plan_no as composite key if both exist
        // Otherwise use lot alone, or assessment_no as fallback
        FOREACH (_ IN CASE WHEN $lot IS NOT NULL AND $plan_no IS NOT NULL THEN [1] ELSE [] END |
            MERGE (pp:PropertyParcel {lot: $lot, plan_no: $plan_no})
            ON CREATE SET
                pp.extent = $extent,
                pp.assessment_no = $assessment_no,
                pp.boundary_north = $boundary_n,
                pp.boundary_east = $boundary_e,
                pp.boundary_south = $boundary_s,
                pp.boundary_west = $boundary_w
            ON MATCH SET
                pp.extent = COALESCE($extent, pp.extent),
                pp.assessment_no = COALESCE($assessment_no, pp.assessment_no),
                pp.boundary_north = COALESCE($boundary_n, pp.boundary_north),
                pp.boundary_east = COALESCE($boundary_e, pp.boundary_east),
                pp.boundary_south = COALESCE($boundary_s, pp.boundary_south),
                pp.boundary_west = COALESCE($boundary_w, pp.boundary_west)
            MERGE (i)-[:CONVEYS]->(pp)
            MERGE (pl2:Plan {plan_no: $plan_no})
            MERGE (pp)-[:DEFINED_BY]->(pl2)
        )

        // Fallback: Create parcel with just lot (no plan_no)
        FOREACH (_ IN CASE WHEN $lot IS NOT NULL AND $plan_no IS NULL THEN [1] ELSE [] END |
            MERGE (pp:PropertyParcel {lot: $lot})
            ON CREATE SET
                pp.extent = $extent,
                pp.assessment_no = $assessment_no,
                pp.boundary_north = $boundary_n,
                pp.boundary_east = $boundary_e,
                pp.boundary_south = $boundary_s,
                pp.boundary_west = $boundary_w
            MERGE (i)-[:CONVEYS]->(pp)
        )

        // Fallback: Create parcel with assessment_no if no lot
        FOREACH (_ IN CASE WHEN $lot IS NULL AND $assessment_no IS NOT NULL THEN [1] ELSE [] END |
            MERGE (pp:PropertyParcel {assessment_no: $assessment_no})
            ON CREATE SET
                pp.extent = $extent,
                pp.boundary_north = $boundary_n,
                pp.boundary_east = $boundary_e,
                pp.boundary_south = $boundary_s,
                pp.boundary_west = $boundary_w
            MERGE (i)-[:CONVEYS]->(pp)
        )

        RETURN i.code_number AS code
        """

        tx.run(
            query,
            code=d["code_number"],
            plan_no=plan_no,
            plan_date=plan_date,
            surveyor=surveyor,
            lot=lot,
            extent=extent,
            assessment_no=assessment_no,
            boundary_n=boundaries.get("N"),
            boundary_e=boundaries.get("E"),
            boundary_s=boundaries.get("S"),
            boundary_w=boundaries.get("W")
        )

        if plan_no:
            self.stats["plans"] += 1
        if lot or assessment_no:
            self.stats["parcels"] += 1

    # =========================================================================
    # PARTIES (PERSONS)
    # =========================================================================
    def merge_parties(self, tx, d: Dict):
        """
        Create Person nodes and role relationships.
        Updated to handle new structure: vendor/vendee/donor/donee/testator objects with 'names' list
        """
        parties = []
        deed_type = d.get("type", "unknown")

        # Sale/Transfer deeds
        if deed_type == "sale_transfer":
            for name in d.get("vendor", {}).get("names", []):
                if name:
                    parties.append({"role": "VENDOR", "name": name, "role_type": "TRANSFEROR"})
            for name in d.get("vendee", {}).get("names", []):
                if name:
                    parties.append({"role": "VENDEE", "name": name, "role_type": "TRANSFEREE"})

        # Gift deeds
        elif deed_type == "gift":
            for name in d.get("donor", {}).get("names", []):
                if name:
                    parties.append({"role": "DONOR", "name": name, "role_type": "TRANSFEROR"})
            for name in d.get("donee", {}).get("names", []):
                if name:
                    parties.append({"role": "DONEE", "name": name, "role_type": "TRANSFEREE"})

        # Wills
        elif deed_type == "will":
            for name in d.get("testator", {}).get("names", []):
                if name:
                    parties.append({"role": "TESTATOR", "name": name, "role_type": "GRANTOR"})
            # Executors if present
            for ex in d.get("executors", []):
                name = ex.get("name") if isinstance(ex, dict) else ex
                if name:
                    parties.append({"role": "EXECUTOR", "name": name, "role_type": "FIDUCIARY"})

        # Mortgage
        elif deed_type == "mortgage":
            for name in d.get("mortgagor", {}).get("names", []):
                if name:
                    parties.append({"role": "MORTGAGOR", "name": name, "role_type": "DEBTOR"})
            for name in d.get("mortgagee", {}).get("names", []):
                if name:
                    parties.append({"role": "MORTGAGEE", "name": name, "role_type": "CREDITOR"})

        # Lease
        elif deed_type == "lease":
            for name in d.get("lessor", {}).get("names", []):
                if name:
                    parties.append({"role": "LESSOR", "name": name, "role_type": "LANDLORD"})
            for name in d.get("lessee", {}).get("names", []):
                if name:
                    parties.append({"role": "LESSEE", "name": name, "role_type": "TENANT"})

        # Notary (common to all deed types)
        notary = d.get("notary", {})
        notary_name = notary.get("name") if isinstance(notary, dict) else notary
        if notary_name:
            parties.append({"role": "NOTARY", "name": notary_name, "role_type": "OFFICIAL"})

        if not parties:
            return

        query = """
        MATCH (i:Instrument {code_number: $code})
        UNWIND $parties AS party
        MERGE (p:Person {name: party.name})
        MERGE (p)-[r:HAS_ROLE]->(i)
        SET r.role = party.role,
            r.role_type = party.role_type
        RETURN count(p) AS person_count
        """

        result = tx.run(query, code=d["code_number"], parties=parties)
        record = result.single()
        if record:
            self.stats["persons"] += record["person_count"]

    # =========================================================================
    # NICs (IDENTIFICATION NUMBERS)
    # =========================================================================
    def merge_nics(self, tx, d: Dict):
        """
        Create NIC nodes and link to instrument.
        New field from hybrid extractor: ids.nic_all
        """
        ids_data = d.get("ids", {})
        nics = ids_data.get("nic_all", []) if isinstance(ids_data, dict) else []

        if not nics:
            return

        query = """
        MATCH (i:Instrument {code_number: $code})
        UNWIND $nics AS nic_number
        MERGE (n:NIC {number: nic_number})
        MERGE (i)-[:CONTAINS_NIC]->(n)
        """

        tx.run(query, code=d["code_number"], nics=nics)

    # =========================================================================
    # PRIOR DEED REFERENCES
    # =========================================================================
    def merge_prior_deed(self, tx, d: Dict):
        """
        Link to prior deed if referenced.
        Field: prior_deed (string)
        """
        prior_deed = d.get("prior_deed")
        if not prior_deed:
            return

        query = """
        MATCH (i:Instrument {code_number: $code})
        MERGE (pd:PriorDeed {reference: $prior_ref})
        MERGE (i)-[:REFERS_TO_PRIOR]->(pd)
        
        // Try to link to actual instrument if it exists
        WITH i, pd
        OPTIONAL MATCH (existing:Instrument)
        WHERE existing.code_number CONTAINS $prior_ref 
           OR existing.id CONTAINS $prior_ref
        FOREACH (_ IN CASE WHEN existing IS NOT NULL THEN [1] ELSE [] END |
            MERGE (i)-[:DERIVES_FROM]->(existing)
        )
        """

        tx.run(query, code=d["code_number"], prior_ref=prior_deed)

    # =========================================================================
    # QUALITY ISSUES (FOR REVIEW TRACKING)
    # =========================================================================
    def merge_quality_issues(self, tx, d: Dict):
        """
        Create QualityIssue nodes for documents needing review.
        Helps track extraction quality for improvement.
        """
        source = d.get("source", {})
        quality = source.get("quality_score", {})

        issues = quality.get("issues", [])
        warnings = quality.get("warnings", [])

        if not issues and not warnings:
            return

        query = """
        MATCH (i:Instrument {code_number: $code})
        
        // Create issue nodes
        FOREACH (issue IN $issues |
            MERGE (qi:QualityIssue {type: 'ERROR', description: issue})
            MERGE (i)-[:HAS_ISSUE]->(qi)
        )
        
        // Create warning nodes
        FOREACH (warning IN $warnings |
            MERGE (qw:QualityIssue {type: 'WARNING', description: warning})
            MERGE (i)-[:HAS_WARNING]->(qw)
        )
        """

        tx.run(query, code=d["code_number"], issues=issues, warnings=warnings)

    # =========================================================================
    # MAIN LOADING FUNCTION
    # =========================================================================
    def load_deed(self, deed_data: Dict) -> bool:
        """Load a single deed into the knowledge graph."""
        try:
            with self.driver.session() as session:
                session.execute_write(self.merge_instrument, deed_data)
                session.execute_write(self.merge_plan_and_parcel, deed_data)
                session.execute_write(self.merge_parties, deed_data)
                session.execute_write(self.merge_nics, deed_data)
                session.execute_write(self.merge_prior_deed, deed_data)
                session.execute_write(self.merge_quality_issues, deed_data)
            return True
        except Exception as e:
            print(f"  ❌ Error loading deed: {e}")
            self.stats["errors"] += 1
            return False

    def load_directory(self, directory: pathlib.Path) -> Dict:
        """Load all JSON files from a directory."""
        files = sorted(directory.glob("*.json"))
        # Exclude summary files
        files = [f for f in files if not f.name.startswith("_")]

        if not files:
            raise SystemExit(f"❌ No JSON files found in {directory}")

        print("=" * 80)
        print("LOADING DEEDS TO NEO4J KNOWLEDGE GRAPH")
        print("=" * 80)
        print(f"Directory: {directory}")
        print(f"Files: {len(files)}")
        print("-" * 80)

        for i, filepath in enumerate(files, 1):
            try:
                raw = filepath.read_text(encoding="utf-8", errors="strict")
                deed_data = json.loads(raw)
            except UnicodeDecodeError as e:
                print(f"[{i}/{len(files)}] ⚠ Encoding issue in {filepath.name}, retrying...")
                raw = filepath.read_text(encoding="utf-8", errors="replace")
                deed_data = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"[{i}/{len(files)}] ❌ JSON error in {filepath.name}: {e}")
                self.stats["errors"] += 1
                continue

            success = self.load_deed(deed_data)
            
            if success:
                quality = deed_data.get("source", {}).get("quality_score", {})
                rating = quality.get("rating", "N/A")
                print(f"[{i}/{len(files)}] ✓ {filepath.name} | {deed_data.get('type')} | {rating}")
            else:
                print(f"[{i}/{len(files)}] ✗ {filepath.name} | FAILED")

        return self.stats

    def print_summary(self):
        """Print loading summary statistics."""
        print("\n" + "=" * 80)
        print("LOADING SUMMARY")
        print("=" * 80)
        print(f"Instruments loaded: {self.stats['instruments']}")
        print(f"Persons created:    {self.stats['persons']}")
        print(f"Parcels created:    {self.stats['parcels']}")
        print(f"Plans created:      {self.stats['plans']}")
        print(f"Errors:             {self.stats['errors']}")
        print("=" * 80)


# =============================================================================
# SCHEMA SETUP (Run once to create indexes and constraints)
# =============================================================================
def setup_schema(driver):
    """Create indexes and constraints for optimal performance."""
    constraints_and_indexes = [
        # Constraints (unique)
        "CREATE CONSTRAINT instrument_code IF NOT EXISTS FOR (i:Instrument) REQUIRE i.code_number IS UNIQUE",
        "CREATE CONSTRAINT person_name IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE",
        "CREATE CONSTRAINT plan_no IF NOT EXISTS FOR (pl:Plan) REQUIRE pl.plan_no IS UNIQUE",
        "CREATE CONSTRAINT registry_name IF NOT EXISTS FOR (r:RegistryOffice) REQUIRE r.name IS UNIQUE",
        "CREATE CONSTRAINT jurisdiction_name IF NOT EXISTS FOR (j:Jurisdiction) REQUIRE j.name IS UNIQUE",
        "CREATE CONSTRAINT district_name IF NOT EXISTS FOR (d:District) REQUIRE d.name IS UNIQUE",
        "CREATE CONSTRAINT province_name IF NOT EXISTS FOR (pv:Province) REQUIRE pv.name IS UNIQUE",
        "CREATE CONSTRAINT nic_number IF NOT EXISTS FOR (n:NIC) REQUIRE n.number IS UNIQUE",
        
        # Indexes for common queries
        "CREATE INDEX instrument_type IF NOT EXISTS FOR (i:Instrument) ON (i.type)",
        "CREATE INDEX instrument_date IF NOT EXISTS FOR (i:Instrument) ON (i.date)",
        "CREATE INDEX instrument_quality IF NOT EXISTS FOR (i:Instrument) ON (i.quality_rating)",
        "CREATE INDEX parcel_lot IF NOT EXISTS FOR (pp:PropertyParcel) ON (pp.lot)",
        "CREATE INDEX parcel_assessment IF NOT EXISTS FOR (pp:PropertyParcel) ON (pp.assessment_no)",
    ]

    print("Setting up Neo4j schema...")
    with driver.session() as session:
        for query in constraints_and_indexes:
            try:
                session.run(query)
                print(f"  ✓ {query[:60]}...")
            except Exception as e:
                # Constraint/index may already exist
                if "already exists" not in str(e).lower():
                    print(f"  ⚠ {e}")
    print("Schema setup complete.\n")


# =============================================================================
# MAIN EXECUTION
# =============================================================================
def main():
    import sys

    # Configuration from args or defaults
    json_dir = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else JSON_DIR

    if not json_dir.exists():
        print(f"❌ Directory not found: {json_dir}")
        print("\nUsage:")
        print("  python neo4j_loader_v2.py [json_directory]")
        print("  python neo4j_loader_v2.py ./deeds/processed_final")
        sys.exit(1)

    if not NEO4J_PASS:
        print("❌ NEO4J_PASS not set. Please set environment variable or update script.")
        sys.exit(1)

    print(f"\nConnecting to Neo4j at: {NEO4J_URI}")

    with LegalKnowledgeGraphLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASS) as loader:
        setup_schema(loader.driver)
        loader.load_directory(json_dir)
        loader.print_summary()

    print("\n✓ Knowledge graph loading complete!")


if __name__ == "__main__":
    main()
