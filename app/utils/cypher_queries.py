"""
Cypher Query Repository
All Neo4j Cypher queries in one place for easy maintenance
"""

from typing import Dict


class CypherQueries:
    """Repository of all Cypher queries."""
    
    # =========================================================================
    # DEED QUERIES
    # =========================================================================
    
    FIND_DEED_DETAILS = """
        MATCH (i:Instrument)
        WHERE i.code_number CONTAINS $code OR i.id CONTAINS $code
        OPTIONAL MATCH (p:Person)-[r:HAS_ROLE]->(i)
        OPTIONAL MATCH (i)-[:CONVEYS]->(pp:PropertyParcel)
        OPTIONAL MATCH (pp)-[:DEFINED_BY]->(pl:Plan)
        OPTIONAL MATCH (i)-[:IN_DISTRICT]->(d:District)
        OPTIONAL MATCH (i)-[:IN_PROVINCE]->(pv:Province)
        OPTIONAL MATCH (i)-[:REGISTERED_AT]->(ro:RegistryOffice)
        OPTIONAL MATCH (i)-[:REFERS_TO_PRIOR]->(pd:PriorDeed)
        OPTIONAL MATCH (i)-[:GOVERNED_BY]->(st:Statute)
        OPTIONAL MATCH (i)-[:MUST_COMPLY_WITH]->(req:DeedRequirement)
        RETURN i.code_number AS deed_code, i.type AS deed_type, i.date AS date,
               i.consideration_lkr AS amount, d.name AS district, pv.name AS province,
               ro.name AS registry, pl.plan_no AS plan_no, pl.plan_date AS plan_date,
               pp.lot AS lot, pp.extent AS extent, pp.assessment_no AS assessment,
               pp.boundary_north AS north, pp.boundary_south AS south,
               pp.boundary_east AS east, pp.boundary_west AS west,
               pd.reference AS prior_deed,
               collect(DISTINCT {name: p.name, role: r.role}) AS parties,
               collect(DISTINCT st.name) AS governing_statutes,
               req.requirements AS requirements
        LIMIT 1
    """
    
    FIND_DEED_PARTIES = """
        MATCH (i:Instrument)
        WHERE i.code_number CONTAINS $code OR i.id CONTAINS $code
        MATCH (p:Person)-[r:HAS_ROLE]->(i)
        OPTIONAL MATCH (p)-[:HAS_NIC]->(n:NIC)
        RETURN i.code_number AS deed_code, i.type AS deed_type, i.date AS date,
               p.name AS person_name, r.role AS role, n.number AS nic
        ORDER BY r.role
    """
    
    FIND_BOUNDARIES = """
        MATCH (i:Instrument)
        WHERE i.code_number CONTAINS $code OR i.id CONTAINS $code
        OPTIONAL MATCH (i)-[:CONVEYS]->(pp:PropertyParcel)
        RETURN i.code_number AS deed_code, i.type AS deed_type, pp.lot AS lot,
               pp.boundary_north AS north, pp.boundary_south AS south,
               pp.boundary_east AS east, pp.boundary_west AS west,
               pp.extent AS extent
        LIMIT 1
    """
    
    FIND_BY_BOUNDARY = """
        MATCH (pp:PropertyParcel)
        WHERE toLower(pp.boundary_north) CONTAINS toLower($name)
           OR toLower(pp.boundary_south) CONTAINS toLower($name)
           OR toLower(pp.boundary_east) CONTAINS toLower($name)
           OR toLower(pp.boundary_west) CONTAINS toLower($name)
        OPTIONAL MATCH (i:Instrument)-[:CONVEYS]->(pp)
        OPTIONAL MATCH (i)-[:IN_DISTRICT]->(d:District)
        OPTIONAL MATCH (p:Person)-[r:HAS_ROLE]->(i)
        RETURN pp.lot AS lot, pp.extent AS extent, pp.assessment_no AS assessment,
               pp.boundary_north AS north, pp.boundary_south AS south,
               pp.boundary_east AS east, pp.boundary_west AS west,
               i.code_number AS deed_code, i.type AS deed_type, i.date AS date,
               d.name AS district,
               collect(DISTINCT {name: p.name, role: r.role}) AS parties
        LIMIT 10
    """
    
    FIND_OWNERSHIP_CHAIN = """
        MATCH (i:Instrument)
        WHERE i.code_number CONTAINS $code OR i.id CONTAINS $code
        OPTIONAL MATCH (i)-[:REFERS_TO_PRIOR]->(pd:PriorDeed)
        OPTIONAL MATCH (i)-[:DERIVES_FROM]->(prior:Instrument)
        OPTIONAL MATCH (p:Person)-[r:HAS_ROLE]->(i)
        OPTIONAL MATCH (i)-[:CONVEYS]->(pp:PropertyParcel)
        OPTIONAL MATCH (pp2:Person)-[r2:HAS_ROLE]->(prior)
        RETURN i.code_number AS current_deed, i.type AS deed_type, i.date AS date,
               pd.reference AS prior_reference,
               prior.code_number AS prior_deed_code, prior.date AS prior_date,
               pp.lot AS lot,
               collect(DISTINCT {name: p.name, role: r.role}) AS current_parties,
               collect(DISTINCT {name: pp2.name, role: r2.role}) AS prior_parties
    """
    
    FIND_PERSON_DEEDS = """
        MATCH (p:Person)-[r:HAS_ROLE]->(i:Instrument)
        WHERE toLower(p.name) CONTAINS toLower($name)
        OPTIONAL MATCH (i)-[:CONVEYS]->(pp:PropertyParcel)
        OPTIONAL MATCH (i)-[:IN_DISTRICT]->(d:District)
        OPTIONAL MATCH (i)-[:GOVERNED_BY]->(st:Statute)
        RETURN p.name AS person, r.role AS role, i.code_number AS deed_code,
               i.type AS deed_type, i.date AS date, i.consideration_lkr AS amount,
               pp.lot AS lot, pp.extent AS extent, d.name AS district,
               collect(DISTINCT st.short_name) AS applicable_laws
        ORDER BY i.date DESC
        LIMIT $limit
    """
    
    FIND_DISTRICT_DEEDS = """
        MATCH (i:Instrument)-[:IN_DISTRICT]->(d:District)
        WHERE toLower(d.name) CONTAINS toLower($district)
        OPTIONAL MATCH (p:Person)-[r:HAS_ROLE]->(i)
        OPTIONAL MATCH (i)-[:CONVEYS]->(pp:PropertyParcel)
        RETURN i.code_number AS deed_code, i.type AS deed_type, i.date AS date,
               i.consideration_lkr AS amount, d.name AS district,
               collect(DISTINCT {name: p.name, role: r.role}) AS parties,
               pp.lot AS lot, pp.extent AS extent
        ORDER BY i.date DESC
        LIMIT $limit
    """
    
    FIND_BY_TYPE = """
        MATCH (i:Instrument)
        WHERE toLower(i.type) CONTAINS toLower($deed_type)
        OPTIONAL MATCH (p:Person)-[r:HAS_ROLE]->(i)
        OPTIONAL MATCH (i)-[:CONVEYS]->(pp:PropertyParcel)
        OPTIONAL MATCH (i)-[:IN_DISTRICT]->(d:District)
        OPTIONAL MATCH (i)-[:GOVERNED_BY]->(st:Statute)
        RETURN i.code_number AS deed_code, i.type AS deed_type, i.date AS date,
               i.consideration_lkr AS amount, d.name AS district,
               pp.lot AS lot, pp.extent AS extent,
               collect(DISTINCT {name: p.name, role: r.role}) AS parties,
               collect(DISTINCT st.short_name) AS governing_laws
        ORDER BY i.date DESC
        LIMIT $limit
    """
    
    FIND_PROPERTY = """
        MATCH (pp:PropertyParcel)
        WHERE pp.lot CONTAINS $lot OR pp.assessment_no CONTAINS $lot
        OPTIONAL MATCH (i:Instrument)-[:CONVEYS]->(pp)
        OPTIONAL MATCH (pp)-[:DEFINED_BY]->(pl:Plan)
        OPTIONAL MATCH (p:Person)-[r:HAS_ROLE]->(i)
        OPTIONAL MATCH (i)-[:IN_DISTRICT]->(d:District)
        RETURN pp.lot AS lot, pp.extent AS extent, pp.assessment_no AS assessment,
               pp.boundary_north AS north, pp.boundary_south AS south,
               pp.boundary_east AS east, pp.boundary_west AS west,
               pl.plan_no AS plan_no, pl.plan_date AS plan_date,
               i.code_number AS deed_code, i.type AS deed_type, i.date AS date,
               d.name AS district,
               collect(DISTINCT {name: p.name, role: r.role}) AS parties
        LIMIT 5
    """
    
    FIND_RECENT_DEEDS = """
        MATCH (i:Instrument)
        WHERE i.date IS NOT NULL
        OPTIONAL MATCH (p:Person)-[r:HAS_ROLE]->(i)
        OPTIONAL MATCH (i)-[:IN_DISTRICT]->(d:District)
        OPTIONAL MATCH (i)-[:CONVEYS]->(pp:PropertyParcel)
        RETURN i.code_number AS deed_code, i.type AS deed_type, i.date AS date,
               i.consideration_lkr AS amount, d.name AS district, pp.lot AS lot,
               collect(DISTINCT {name: p.name, role: r.role}) AS parties
        ORDER BY i.date DESC
        LIMIT $limit
    """
    
    FIND_BY_AMOUNT = """
        MATCH (i:Instrument)
        WHERE i.consideration_lkr IS NOT NULL AND i.consideration_lkr > 0
        OPTIONAL MATCH (p:Person)-[r:HAS_ROLE]->(i)
        OPTIONAL MATCH (i)-[:CONVEYS]->(pp:PropertyParcel)
        OPTIONAL MATCH (i)-[:IN_DISTRICT]->(d:District)
        RETURN i.code_number AS deed_code, i.type AS deed_type, i.date AS date,
               i.consideration_lkr AS amount, d.name AS district, pp.lot AS lot,
               collect(DISTINCT {name: p.name, role: r.role}) AS parties
        ORDER BY i.consideration_lkr DESC
        LIMIT $limit
    """
    
    # =========================================================================
    # LEGAL/STATUTE QUERIES
    # =========================================================================
    
    FIND_STATUTE = """
        MATCH (s:Statute)
        WHERE toLower(s.name) CONTAINS toLower($query)
           OR toLower(s.short_name) CONTAINS toLower($query)
           OR toLower(s.category) CONTAINS toLower($query)
        OPTIONAL MATCH (sec:Section)-[:PART_OF]->(s)
        RETURN s.name AS statute_name, s.short_name AS short_name,
               s.act_number AS act_number, s.year AS year,
               s.category AS category, s.description AS description,
               s.key_provisions AS key_provisions,
               s.applies_to AS applies_to,
               collect(DISTINCT {section: sec.section_number, title: sec.title, content: sec.content}) AS sections
        LIMIT 5
    """
    
    FIND_GOVERNING_LAW = """
        MATCH (i:Instrument)
        WHERE i.code_number CONTAINS $code OR i.id CONTAINS $code
        OPTIONAL MATCH (i)-[:GOVERNED_BY]->(s:Statute)
        OPTIONAL MATCH (i)-[:MUST_COMPLY_WITH]->(req:DeedRequirement)
        OPTIONAL MATCH (sec:Section)-[:PART_OF]->(s)
        WHERE sec.importance = 'critical'
        RETURN i.code_number AS deed_code, i.type AS deed_type,
               collect(DISTINCT {
                   name: s.name, 
                   short_name: s.short_name,
                   description: s.description,
                   key_provisions: s.key_provisions
               }) AS governing_statutes,
               req.requirements AS requirements,
               req.stamp_duty AS stamp_duty,
               collect(DISTINCT {section: sec.section_number, title: sec.title, content: sec.content}) AS critical_sections
    """
    
    FIND_DEED_REQUIREMENTS = """
        MATCH (req:DeedRequirement)
        WHERE toLower(req.deed_type) CONTAINS toLower($deed_type)
        OPTIONAL MATCH (req)-[:GOVERNED_BY]->(s:Statute)
        RETURN req.deed_type AS deed_type, req.name AS requirement_name,
               req.requirements AS requirements,
               req.stamp_duty AS stamp_duty,
               req.registration_fee AS registration_fee,
               collect(DISTINCT s.name) AS governing_statutes
    """
    
    FIND_STATUTES_FOR_DEED_TYPE = """
        MATCH (s:Statute)
        WHERE $deed_type IN s.applies_to
        OPTIONAL MATCH (sec:Section)-[:PART_OF]->(s)
        WHERE sec.importance = 'critical'
        RETURN s.name AS statute_name, s.short_name AS short_name,
               s.act_number AS act_number, s.year AS year,
               s.description AS description,
               s.key_provisions AS key_provisions,
               collect(DISTINCT {section: sec.section_number, title: sec.title}) AS critical_sections
        ORDER BY s.year
    """
    
    FIND_SECTION = """
        MATCH (sec:Section)-[:PART_OF]->(s:Statute)
        WHERE toLower(sec.title) CONTAINS toLower($query)
           OR toLower(sec.content) CONTAINS toLower($query)
           OR sec.section_number CONTAINS $query
        RETURN sec.section_number AS section, sec.title AS title,
               sec.content AS content, sec.importance AS importance,
               s.name AS statute_name, s.short_name AS short_name
        LIMIT 5
    """
    
    FIND_ALL_STATUTES = """
        MATCH (s:Statute)
        OPTIONAL MATCH (sec:Section)-[:PART_OF]->(s)
        RETURN s.name AS statute_name, s.short_name AS short_name,
               s.act_number AS act_number, s.year AS year,
               s.category AS category, s.description AS description,
               s.applies_to AS applies_to,
               count(sec) AS section_count
        ORDER BY s.year
    """
    
    # =========================================================================
    # DEFINITION QUERIES
    # =========================================================================
    
    FIND_DEFINITION = """
        MATCH (d:LegalDefinition)
        WHERE toLower(d.term) CONTAINS toLower($term)
        RETURN d.term AS term, d.definition AS definition, d.source AS source
        LIMIT 5
    """
    
    FIND_ALL_DEFINITIONS = """
        MATCH (d:LegalDefinition)
        RETURN d.term AS term, d.definition AS definition, d.source AS source
        ORDER BY d.term
    """
    
    # =========================================================================
    # LEGAL PRINCIPLE QUERIES
    # =========================================================================
    
    FIND_PRINCIPLE = """
        MATCH (p:LegalPrinciple)
        WHERE toLower(p.name) CONTAINS toLower($query)
           OR toLower(p.english) CONTAINS toLower($query)
           OR toLower(p.description) CONTAINS toLower($query)
        RETURN p.name AS principle_name, p.english AS english_meaning,
               p.description AS description, p.application AS application
        LIMIT 5
    """
    
    FIND_ALL_PRINCIPLES = """
        MATCH (p:LegalPrinciple)
        RETURN p.name AS principle_name, p.english AS english_meaning,
               p.description AS description, p.application AS application
        ORDER BY p.name
    """
    
    # =========================================================================
    # COMPLIANCE QUERIES
    # =========================================================================
    
    CHECK_DEED_COMPLIANCE = """
        MATCH (i:Instrument)
        WHERE i.code_number CONTAINS $code OR i.id CONTAINS $code
        OPTIONAL MATCH (i)-[:MUST_COMPLY_WITH]->(req:DeedRequirement)
        OPTIONAL MATCH (i)-[:GOVERNED_BY]->(s:Statute)
        OPTIONAL MATCH (i)-[:CONVEYS]->(pp:PropertyParcel)
        OPTIONAL MATCH (pp)-[:DEFINED_BY]->(pl:Plan)
        OPTIONAL MATCH (i)-[:REGISTERED_AT]->(ro:RegistryOffice)
        OPTIONAL MATCH (p:Person)-[r:HAS_ROLE]->(i)
        RETURN i.code_number AS deed_code, i.type AS deed_type, i.date AS date,
               i.consideration_lkr AS amount,
               req.requirements AS required_items,
               req.stamp_duty AS stamp_duty_rule,
               collect(DISTINCT s.name) AS governing_statutes,
               pp.lot AS lot, pp.extent AS extent,
               pl.plan_no AS plan_no,
               ro.name AS registry,
               collect(DISTINCT {name: p.name, role: r.role}) AS parties
    """
    
    # =========================================================================
    # STATISTICS
    # =========================================================================
    
    GET_STATS = """
        MATCH (i:Instrument)
        WITH count(i) AS total_deeds
        OPTIONAL MATCH (p:Person)
        WITH total_deeds, count(DISTINCT p) AS total_persons
        OPTIONAL MATCH (pp:PropertyParcel)
        WITH total_deeds, total_persons, count(pp) AS total_parcels
        OPTIONAL MATCH (d:District)
        WITH total_deeds, total_persons, total_parcels, count(DISTINCT d) AS total_districts
        OPTIONAL MATCH (s:Statute)
        WITH total_deeds, total_persons, total_parcels, total_districts, count(s) AS total_statutes
        OPTIONAL MATCH (def:LegalDefinition)
        WITH total_deeds, total_persons, total_parcels, total_districts, total_statutes, count(def) AS total_definitions
        OPTIONAL MATCH (i2:Instrument)
        RETURN total_deeds, total_persons, total_parcels, total_districts,
               total_statutes, total_definitions,
               count(CASE WHEN i2.type = 'sale_transfer' THEN 1 END) AS sales,
               count(CASE WHEN i2.type = 'gift' THEN 1 END) AS gifts,
               count(CASE WHEN i2.type = 'will' THEN 1 END) AS wills,
               count(CASE WHEN i2.type = 'lease' THEN 1 END) AS leases,
               count(CASE WHEN i2.type = 'mortgage' THEN 1 END) AS mortgages
    """
    
    # =========================================================================
    # GENERAL SEARCH
    # =========================================================================
    
    GENERAL_SEARCH = """
        CALL {
            MATCH (p:Person)
            WHERE toLower(p.name) CONTAINS toLower($query)
            RETURN 'Person' AS type, p.name AS name, null AS code, null AS extra
            UNION
            MATCH (i:Instrument)
            WHERE i.code_number CONTAINS $query OR i.id CONTAINS $query
                   OR toLower(i.type) CONTAINS toLower($query)
            RETURN 'Deed' AS type, i.type AS name, i.code_number AS code, i.date AS extra
            UNION
            MATCH (d:District)
            WHERE toLower(d.name) CONTAINS toLower($query)
            RETURN 'District' AS type, d.name AS name, null AS code, null AS extra
            UNION
            MATCH (pp:PropertyParcel)
            WHERE pp.lot CONTAINS $query OR pp.assessment_no CONTAINS $query
            RETURN 'Property' AS type, pp.lot AS name, pp.assessment_no AS code, pp.extent AS extra
            UNION
            MATCH (s:Statute)
            WHERE toLower(s.name) CONTAINS toLower($query)
               OR toLower(s.short_name) CONTAINS toLower($query)
            RETURN 'Statute' AS type, s.name AS name, s.act_number AS code, toString(s.year) AS extra
            UNION
            MATCH (def:LegalDefinition)
            WHERE toLower(def.term) CONTAINS toLower($query)
            RETURN 'Definition' AS type, def.term AS name, null AS code, def.source AS extra
            UNION
            MATCH (prin:LegalPrinciple)
            WHERE toLower(prin.name) CONTAINS toLower($query)
               OR toLower(prin.english) CONTAINS toLower($query)
            RETURN 'Legal Principle' AS type, prin.name AS name, null AS code, prin.english AS extra
        }
        RETURN type, name, code, extra
        LIMIT 15
    """


# Create singleton instance
queries = CypherQueries()
