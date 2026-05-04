"""
Neo4j Loader for LegalVision Knowledge Graph — PATCHED
======================================================
Same public API as the original neo4j_loader_v2.py:
    LegalKnowledgeGraphLoader, setup_schema

Difference from the original:
  - Boundaries are no longer stored as string properties on PropertyParcel.
  - Each direction becomes a (:PropertyParcel)-[:BOUNDED_BY {direction, raw_text, kind}]->(neighbour)
    relationship, where the neighbour is one of: another PropertyParcel
    (lot or claimed-by-person), a BoundaryFeature (road/lake/drain), or an
    UnknownBoundary (fallback).
  - Adds CLAIMED_BY edges from neighbour parcels to the Person who claims them.

Drop-in replacement: same import line, same constructor, same load_directory().
"""

import os
import json
import pathlib
import re
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

from neo4j import GraphDatabase

# Configuration
JSON_DIR = pathlib.Path("./deeds/processed_final")  
NEO4J_URI="neo4j+s://9767182a.databases.neo4j.io"
NEO4J_USER="9767182a"
NEO4J_PASS="eoyduAgumvZzn2FVMnGNLAvErder8zKZSTcKT8nxUuk"


# =============================================================================
# BOUNDARY CLASSIFICATION
# =============================================================================

_PARCEL_OWNER_PATS = [
    re.compile(r"\bproperty\s+(?:claimed\s+by|of)\s+(.+)", re.I),
    re.compile(r"\bthe\s+land\s+of\s+(.+)", re.I),
    re.compile(r"\bproperty\s+belonging\s+to\s+(.+)", re.I),
]

_FEATURE_PATS = [
    (re.compile(r"\b(.*\s*road)\b", re.I),                      "ROAD"),
    (re.compile(r"\b(.*\s*lane)\b", re.I),                      "ROAD"),
    (re.compile(r"\b(.*\s*mawatha)\b", re.I),                   "ROAD"),
    (re.compile(r"\b(.*\s*highway)\b", re.I),                   "ROAD"),
    (re.compile(r"\blake\b", re.I),                             "WATER_BODY"),
    (re.compile(r"\b(canal|stream|river|tank)\b", re.I),        "WATER_BODY"),
    (re.compile(r"\breservation\s+for\s+(drains?)\b", re.I),    "DRAIN_RESERVATION"),
    (re.compile(r"\breservation\s+for\s+(.*roadway)\b", re.I),  "ROADWAY_RESERVATION"),
    (re.compile(r"\b(.*drainage\s+reservation)\b", re.I),       "DRAIN_RESERVATION"),
]


def _norm(name: str) -> str:
    if not name:
        return ""
    n = re.sub(r"^(mr|mrs|ms|dr|miss|prof)\.?\s+", "", name.lower().strip())
    return re.sub(r"\s+", " ", n)


def classify_boundary(raw_text: str, current_plan: Optional[str] = None) -> dict:
    if not raw_text or not raw_text.strip():
        return {"kind": "unknown", "raw_text": raw_text}

    text = raw_text.strip().rstrip(".,;").strip()
    text_lc = text.lower()

    # 1) Lot reference
    m_lot = re.search(r"\blot\s+(\d+)\b", text, re.I)
    if m_lot:
        m_plan = re.search(r"\bplan\s+no\.?\s*([A-Za-z0-9/\-]+)", text, re.I)
        target_plan = None
        if m_plan:
            target_plan = m_plan.group(1)
        elif "same plan" in text_lc or ("plan no" not in text_lc and "in plan" not in text_lc):
            target_plan = current_plan
        if target_plan:
            return {
                "kind": "parcel_lot",
                "lot_number": m_lot.group(1),
                "plan_no": target_plan,
                "raw_text": raw_text,
            }

    # 2) Parcel claimed by named person
    for pat in _PARCEL_OWNER_PATS:
        m = pat.search(text)
        if m:
            owner = m.group(1).strip().rstrip(".,;").strip()
            owner = re.split(r"\s+and\s+", owner, maxsplit=1)[0].strip()
            if re.search(r"\b(road|lane|mawatha|highway|lake|canal|stream|river|reservation)\b",
                         owner, re.I):
                continue
            return {
                "kind": "parcel_owner",
                "owner_name": owner,
                "owner_normalised": _norm(owner),
                "raw_text": raw_text,
            }

    # 3) Physical feature
    cleaned = re.sub(r"^\s*(by|on|along|to)\s+(the\s+)?", "", text, flags=re.I)
    for pat, ftype in _FEATURE_PATS:
        m = pat.search(cleaned)
        if m:
            name = (m.group(1) if m.lastindex else m.group(0)).strip().title()
            name = re.sub(r"^(By|The)\s+", "", name)
            return {
                "kind": "feature",
                "feature_type": ftype,
                "feature_name": name,
                "raw_text": raw_text,
            }

    return {"kind": "unknown", "raw_text": raw_text}


# =============================================================================
# LOADER
# =============================================================================

class LegalKnowledgeGraphLoader:
    """Loads extracted deed JSON into the Neo4j knowledge graph."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.stats = {
            "instruments": 0,
            "persons": 0,
            "parcels": 0,
            "plans": 0,
            "boundaries": 0,
            "errors": 0,
        }

    def close(self):
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ------------------------------------------------------------------
    # INSTRUMENT
    # ------------------------------------------------------------------
    def merge_instrument(self, tx, d: Dict):
        query = """
        MERGE (i:Instrument {code_number: $code})
        ON CREATE SET
            i.id = $id, i.type = $type, i.date = $date,
            i.consideration_lkr = $consideration, i.prior_deed = $prior_deed,
            i.extraction_method = $extraction_method,
            i.quality_score = $quality_score,
            i.quality_rating = $quality_rating,
            i.needs_review = $needs_review,
            i.created_at = datetime()
        ON MATCH SET
            i.type = $type, i.date = $date,
            i.consideration_lkr = $consideration, i.prior_deed = $prior_deed,
            i.extraction_method = $extraction_method,
            i.quality_score = $quality_score,
            i.quality_rating = $quality_rating,
            i.needs_review = $needs_review,
            i.updated_at = datetime()

        FOREACH (_ IN CASE WHEN $registry IS NOT NULL THEN [1] ELSE [] END |
            MERGE (r:RegistryOffice {name: $registry})
            MERGE (i)-[:REGISTERED_AT]->(r))

        FOREACH (_ IN CASE WHEN $jurisdiction IS NOT NULL THEN [1] ELSE [] END |
            MERGE (j:Jurisdiction {name: $jurisdiction})
            MERGE (i)-[:UNDER_JURISDICTION]->(j))

        FOREACH (_ IN CASE WHEN $district IS NOT NULL THEN [1] ELSE [] END |
            MERGE (d:District {name: $district})
            MERGE (i)-[:IN_DISTRICT]->(d))

        FOREACH (_ IN CASE WHEN $province IS NOT NULL THEN [1] ELSE [] END |
            MERGE (pv:Province {name: $province})
            MERGE (i)-[:IN_PROVINCE]->(pv)
            FOREACH (__ IN CASE WHEN $district IS NOT NULL THEN [1] ELSE [] END |
                MERGE (dd:District {name: $district})
                MERGE (dd)-[:LOCATED_IN]->(pv)))
        """
        source = d.get("source", {})
        quality = source.get("quality_score", {})
        tx.run(
            query,
            code=d["code_number"], id=d.get("id"), type=d.get("type"),
            date=d.get("date"), consideration=d.get("consideration_lkr"),
            prior_deed=d.get("prior_deed"),
            registry=d.get("registry_office"), jurisdiction=d.get("jurisdiction"),
            district=d.get("district"), province=d.get("province"),
            extraction_method=source.get("extraction_method"),
            quality_score=quality.get("percentage"),
            quality_rating=quality.get("rating"),
            needs_review=quality.get("needs_review", False),
        )
        self.stats["instruments"] += 1

    # ------------------------------------------------------------------
    # PLAN + PARCEL + BOUNDARIES (the patched method)
    # ------------------------------------------------------------------
    def merge_plan_and_parcel(self, tx, d: Dict):
        plan = d.get("plan", {}) or {}
        prop = d.get("property", {}) or {}

        plan_no       = plan.get("plan_no")
        plan_date     = plan.get("plan_date")
        surveyor      = plan.get("surveyor")

        lot           = prop.get("lot")
        extent        = prop.get("extent")
        assessment_no = prop.get("assessment_no")
        boundaries    = prop.get("boundaries", {}) or {}

        # ---- Step A: Plan + subject parcel ----
        tx.run(
            """
            MATCH (i:Instrument {code_number: $code})

            FOREACH (_ IN CASE WHEN $plan_no IS NOT NULL THEN [1] ELSE [] END |
                MERGE (pl:Plan {plan_no: $plan_no})
                ON CREATE SET pl.plan_date = $plan_date, pl.surveyor = $surveyor
                ON MATCH  SET pl.plan_date = COALESCE($plan_date, pl.plan_date),
                              pl.surveyor  = COALESCE($surveyor,  pl.surveyor)
                MERGE (i)-[:REFERENCES_PLAN]->(pl))

            FOREACH (_ IN CASE WHEN $lot IS NOT NULL AND $plan_no IS NOT NULL THEN [1] ELSE [] END |
                MERGE (pp:PropertyParcel {lot: $lot, plan_no: $plan_no})
                ON CREATE SET pp.extent = $extent, pp.assessment_no = $assessment_no
                ON MATCH  SET pp.extent = COALESCE($extent, pp.extent),
                              pp.assessment_no = COALESCE($assessment_no, pp.assessment_no)
                MERGE (i)-[:CONVEYS]->(pp)
                MERGE (pl2:Plan {plan_no: $plan_no})
                MERGE (pp)-[:DEFINED_BY]->(pl2))

            FOREACH (_ IN CASE WHEN $lot IS NOT NULL AND $plan_no IS NULL THEN [1] ELSE [] END |
                MERGE (pp:PropertyParcel {lot: $lot})
                ON CREATE SET pp.extent = $extent, pp.assessment_no = $assessment_no
                MERGE (i)-[:CONVEYS]->(pp))

            FOREACH (_ IN CASE WHEN $lot IS NULL AND $assessment_no IS NOT NULL THEN [1] ELSE [] END |
                MERGE (pp:PropertyParcel {assessment_no: $assessment_no})
                ON CREATE SET pp.extent = $extent
                MERGE (i)-[:CONVEYS]->(pp))
            """,
            code=d["code_number"],
            plan_no=plan_no, plan_date=plan_date, surveyor=surveyor,
            lot=lot, extent=extent, assessment_no=assessment_no,
        )
        if plan_no:
            self.stats["plans"] += 1
        if lot or assessment_no:
            self.stats["parcels"] += 1

        # ---- Step B: Boundary edges ----
        if lot and plan_no:
            subject_match = "(pp:PropertyParcel {lot: $lot, plan_no: $plan_no})"
            sp = {"lot": lot, "plan_no": plan_no}
        elif lot:
            subject_match = "(pp:PropertyParcel {lot: $lot})"
            sp = {"lot": lot}
        elif assessment_no:
            subject_match = "(pp:PropertyParcel {assessment_no: $assessment_no})"
            sp = {"assessment_no": assessment_no}
        else:
            return  # No anchor parcel — skip boundaries

        direction_map = {"N": "NORTH", "E": "EAST", "S": "SOUTH", "W": "WEST"}

        for short, full in direction_map.items():
            raw = boundaries.get(short)
            if not raw:
                continue
            c = classify_boundary(raw, current_plan=plan_no)

            if c["kind"] == "parcel_lot":
                tx.run(
                    f"""
                    MATCH {subject_match}
                    MERGE (n:PropertyParcel {{lot: $n_lot, plan_no: $n_plan}})
                    MERGE (pp)-[r:BOUNDED_BY {{direction: $dir}}]->(n)
                    SET   r.raw_text = $raw, r.kind = 'parcel_lot'
                    """,
                    **sp, n_lot=c["lot_number"], n_plan=c.get("plan_no") or plan_no,
                    dir=full, raw=raw,
                )
                self.stats["boundaries"] += 1

            elif c["kind"] == "parcel_owner":
                tx.run(
                    f"""
                    MATCH {subject_match}
                    MERGE (owner:Person {{name: $owner_name}})
                    MERGE (n:PropertyParcel {{neighbour_key: $n_key}})
                    ON CREATE SET n.claimed_by_name   = $owner_name,
                                  n.is_neighbour_only = true
                    MERGE (n)-[:CLAIMED_BY]->(owner)
                    MERGE (pp)-[r:BOUNDED_BY {{direction: $dir}}]->(n)
                    SET   r.raw_text = $raw, r.kind = 'parcel_owner'
                    """,
                    **sp,
                    owner_name=c["owner_name"],
                    n_key=f"claimed_by::{c['owner_normalised']}",
                    dir=full, raw=raw,
                )
                self.stats["boundaries"] += 1

            elif c["kind"] == "feature":
                tx.run(
                    f"""
                    MATCH {subject_match}
                    MERGE (b:BoundaryFeature {{feature_key: $f_key}})
                    ON CREATE SET b.name = $f_name, b.feature_type = $f_type
                    MERGE (pp)-[r:BOUNDED_BY {{direction: $dir}}]->(b)
                    SET   r.raw_text = $raw, r.kind = 'feature'
                    """,
                    **sp,
                    f_key=f"{c['feature_type']}::{_norm(c['feature_name'])}",
                    f_name=c["feature_name"], f_type=c["feature_type"],
                    dir=full, raw=raw,
                )
                self.stats["boundaries"] += 1

            else:
                tx.run(
                    f"""
                    MATCH {subject_match}
                    MERGE (u:UnknownBoundary {{raw_text: $raw}})
                    MERGE (pp)-[r:BOUNDED_BY {{direction: $dir}}]->(u)
                    SET   r.kind = 'unknown'
                    """,
                    **sp, dir=full, raw=raw,
                )
                self.stats["boundaries"] += 1

    # ------------------------------------------------------------------
    # PARTIES, NICs, PRIOR DEED, QUALITY ISSUES — unchanged from original
    # ------------------------------------------------------------------
    def merge_parties(self, tx, d: Dict):
        parties = []
        deed_type = d.get("type", "unknown")
        if deed_type == "sale_transfer":
            for n in d.get("vendor", {}).get("names", []):
                if n: parties.append({"role": "VENDOR", "name": n, "role_type": "TRANSFEROR"})
            for n in d.get("vendee", {}).get("names", []):
                if n: parties.append({"role": "VENDEE", "name": n, "role_type": "TRANSFEREE"})
        elif deed_type == "gift":
            for n in d.get("donor", {}).get("names", []):
                if n: parties.append({"role": "DONOR", "name": n, "role_type": "TRANSFEROR"})
            for n in d.get("donee", {}).get("names", []):
                if n: parties.append({"role": "DONEE", "name": n, "role_type": "TRANSFEREE"})
        elif deed_type == "will":
            for n in d.get("testator", {}).get("names", []):
                if n: parties.append({"role": "TESTATOR", "name": n, "role_type": "GRANTOR"})
            for ex in d.get("executors", []):
                n = ex.get("name") if isinstance(ex, dict) else ex
                if n: parties.append({"role": "EXECUTOR", "name": n, "role_type": "FIDUCIARY"})
        elif deed_type == "mortgage":
            for n in d.get("mortgagor", {}).get("names", []):
                if n: parties.append({"role": "MORTGAGOR", "name": n, "role_type": "DEBTOR"})
            for n in d.get("mortgagee", {}).get("names", []):
                if n: parties.append({"role": "MORTGAGEE", "name": n, "role_type": "CREDITOR"})
        elif deed_type == "lease":
            for n in d.get("lessor", {}).get("names", []):
                if n: parties.append({"role": "LESSOR", "name": n, "role_type": "LANDLORD"})
            for n in d.get("lessee", {}).get("names", []):
                if n: parties.append({"role": "LESSEE", "name": n, "role_type": "TENANT"})
        notary = d.get("notary", {})
        notary_name = notary.get("name") if isinstance(notary, dict) else notary
        if notary_name:
            parties.append({"role": "NOTARY", "name": notary_name, "role_type": "OFFICIAL"})
        if not parties:
            return
        result = tx.run(
            """
            MATCH (i:Instrument {code_number: $code})
            UNWIND $parties AS party
            MERGE (p:Person {name: party.name})
            MERGE (p)-[r:HAS_ROLE]->(i)
            SET r.role = party.role, r.role_type = party.role_type
            RETURN count(p) AS person_count
            """,
            code=d["code_number"], parties=parties,
        )
        rec = result.single()
        if rec:
            self.stats["persons"] += rec["person_count"]

    def merge_nics(self, tx, d: Dict):
        ids_data = d.get("ids", {})
        nics = ids_data.get("nic_all", []) if isinstance(ids_data, dict) else []
        if not nics:
            return
        tx.run(
            """
            MATCH (i:Instrument {code_number: $code})
            UNWIND $nics AS nic_number
            MERGE (n:NIC {number: nic_number})
            MERGE (i)-[:CONTAINS_NIC]->(n)
            """,
            code=d["code_number"], nics=nics,
        )

    def merge_prior_deed(self, tx, d: Dict):
        prior_deed = d.get("prior_deed")
        if not prior_deed:
            return
        tx.run(
            """
            MATCH (i:Instrument {code_number: $code})
            MERGE (pd:PriorDeed {reference: $prior_ref})
            MERGE (i)-[:REFERS_TO_PRIOR]->(pd)
            WITH i, pd
            OPTIONAL MATCH (existing:Instrument)
            WHERE existing.code_number CONTAINS $prior_ref OR existing.id CONTAINS $prior_ref
            FOREACH (_ IN CASE WHEN existing IS NOT NULL THEN [1] ELSE [] END |
                MERGE (i)-[:DERIVES_FROM]->(existing))
            """,
            code=d["code_number"], prior_ref=prior_deed,
        )

    def merge_quality_issues(self, tx, d: Dict):
        source = d.get("source", {})
        quality = source.get("quality_score", {})
        issues = quality.get("issues", [])
        warnings = quality.get("warnings", [])
        if not issues and not warnings:
            return
        tx.run(
            """
            MATCH (i:Instrument {code_number: $code})
            FOREACH (issue IN $issues |
                MERGE (qi:QualityIssue {type: 'ERROR', description: issue})
                MERGE (i)-[:HAS_ISSUE]->(qi))
            FOREACH (warning IN $warnings |
                MERGE (qw:QualityIssue {type: 'WARNING', description: warning})
                MERGE (i)-[:HAS_WARNING]->(qw))
            """,
            code=d["code_number"], issues=issues, warnings=warnings,
        )

    # ------------------------------------------------------------------
    # ORCHESTRATION
    # ------------------------------------------------------------------
    def load_deed(self, deed_data: Dict) -> bool:
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
        files = sorted(directory.glob("*.json"))
        files = [f for f in files if not f.name.startswith("_")]
        if not files:
            raise SystemExit(f"❌ No JSON files found in {directory}")

        print("=" * 80)
        print("LOADING DEEDS TO NEO4J KNOWLEDGE GRAPH (with first-class boundaries)")
        print("=" * 80)
        print(f"Directory: {directory}")
        print(f"Files: {len(files)}")
        print("-" * 80)

        for i, fp in enumerate(files, 1):
            try:
                raw = fp.read_text(encoding="utf-8", errors="strict")
                deed_data = json.loads(raw)
            except UnicodeDecodeError:
                raw = fp.read_text(encoding="utf-8", errors="replace")
                deed_data = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"[{i}/{len(files)}] ❌ JSON error in {fp.name}: {e}")
                self.stats["errors"] += 1
                continue
            ok = self.load_deed(deed_data)
            rating = deed_data.get("source", {}).get("quality_score", {}).get("rating", "N/A")
            tag = "✓" if ok else "✗"
            print(f"[{i}/{len(files)}] {tag} {fp.name} | {deed_data.get('type')} | {rating}")
        return self.stats

    def print_summary(self):
        print("\n" + "=" * 80)
        print("LOADING SUMMARY")
        print("=" * 80)
        for k, v in self.stats.items():
            print(f"  {k:14s}: {v}")
        print("=" * 80)


# =============================================================================
# SCHEMA
# =============================================================================

def setup_schema(driver):
    """Create indexes and constraints. Adds new constraints for boundary nodes."""
    statements = [
        # Original constraints
        "CREATE CONSTRAINT instrument_code IF NOT EXISTS FOR (i:Instrument) REQUIRE i.code_number IS UNIQUE",
        "CREATE CONSTRAINT person_name IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS UNIQUE",
        "CREATE CONSTRAINT plan_no IF NOT EXISTS FOR (pl:Plan) REQUIRE pl.plan_no IS UNIQUE",
        "CREATE CONSTRAINT registry_name IF NOT EXISTS FOR (r:RegistryOffice) REQUIRE r.name IS UNIQUE",
        "CREATE CONSTRAINT jurisdiction_name IF NOT EXISTS FOR (j:Jurisdiction) REQUIRE j.name IS UNIQUE",
        "CREATE CONSTRAINT district_name IF NOT EXISTS FOR (d:District) REQUIRE d.name IS UNIQUE",
        "CREATE CONSTRAINT province_name IF NOT EXISTS FOR (pv:Province) REQUIRE pv.name IS UNIQUE",
        "CREATE CONSTRAINT nic_number IF NOT EXISTS FOR (n:NIC) REQUIRE n.number IS UNIQUE",

        # NEW — boundary-related uniqueness
        "CREATE CONSTRAINT parcel_neighbour_key IF NOT EXISTS FOR (pp:PropertyParcel) REQUIRE pp.neighbour_key IS UNIQUE",
        "CREATE CONSTRAINT feature_key IF NOT EXISTS FOR (b:BoundaryFeature) REQUIRE b.feature_key IS UNIQUE",

        # Indexes
        "CREATE INDEX instrument_type IF NOT EXISTS FOR (i:Instrument) ON (i.type)",
        "CREATE INDEX instrument_date IF NOT EXISTS FOR (i:Instrument) ON (i.date)",
        "CREATE INDEX instrument_quality IF NOT EXISTS FOR (i:Instrument) ON (i.quality_rating)",
        "CREATE INDEX parcel_lot IF NOT EXISTS FOR (pp:PropertyParcel) ON (pp.lot)",
        "CREATE INDEX parcel_assessment IF NOT EXISTS FOR (pp:PropertyParcel) ON (pp.assessment_no)",
        # NEW — composite index for the most common parcel lookup
        "CREATE INDEX parcel_lot_plan IF NOT EXISTS FOR (pp:PropertyParcel) ON (pp.lot, pp.plan_no)",
    ]

    print("Setting up Neo4j schema (with boundary-graph constraints)...")
    with driver.session() as session:
        for q in statements:
            try:
                session.run(q)
                print(f"  ✓ {q[:60]}...")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"  ⚠ {e}")
    print("Schema setup complete.\n")


# =============================================================================
# MAIN
# =============================================================================

def main():
    import sys
    json_dir = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else JSON_DIR
    if not json_dir.exists():
        print(f"❌ Directory not found: {json_dir}")
        sys.exit(1)
    if not NEO4J_PASS:
        print("❌ NEO4J_PASS not set.")
        sys.exit(1)
    print(f"\nConnecting to Neo4j at: {NEO4J_URI}")
    with LegalKnowledgeGraphLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASS) as loader:
        setup_schema(loader.driver)
        loader.load_directory(json_dir)
        loader.print_summary()
    print("\n✓ Knowledge graph loading complete!")


if __name__ == "__main__":
    main()