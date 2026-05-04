import re
import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import spacy
from collections import defaultdict


class ImprovedHybridDeedExtractor:
    """
    Enhanced hybrid extractor combining SpaCy NER + Advanced Rule-based patterns.
    Includes folder processing and quality validation.
    """
    
    def __init__(self, spacy_model_path: Optional[str] = None):
        """Initialize with optional SpaCy model."""
        self.use_spacy = False
        self.nlp = None
        
        if spacy_model_path:
            try:
                self.nlp = spacy.load(spacy_model_path)
                self.use_spacy = True
                print(f"✓ Loaded SpaCy model: {spacy_model_path}")
            except Exception as e:
                print(f"⚠ Could not load SpaCy model: {e}")
                print("  Using rule-based extraction only")
        
        # Enhanced multi-pattern extraction
        self.patterns = self._initialize_patterns()
    
    def _initialize_patterns(self) -> Dict:
        """Initialize comprehensive extraction patterns."""
        return {
            # Party patterns - Multiple variations
            'party_vendor': [
                re.compile(r'VENDOR[:\s]+([A-Z][A-Z\s.]+?)\s+\((?:holder|Holder)', re.IGNORECASE),
                re.compile(r'I,\s+([A-Z][A-Z\s.]+?)\s+\(holder of National', re.IGNORECASE),
                re.compile(r'within[- ]named\s+(?:VENDOR|Vendor)\s+([A-Z][A-Z\s.]+)', re.IGNORECASE),
            ],
            'party_vendee': [
                re.compile(r'VENDEE[:\s]+([A-Z][A-Z\s.]+?)\s+\((?:holder|Holder)', re.IGNORECASE),
                re.compile(r'PURCHASER[:\s]+([A-Z][A-Z\s.]+?)\s+\((?:holder|Holder)', re.IGNORECASE),
            ],
            'party_donor': [
                re.compile(r'DONOR[:\s]+([A-Z][A-Z\s.]+?)\s+(?:of|both)', re.IGNORECASE),
            ],
            'party_donee': [
                re.compile(r'DONEE[:\s]+([A-Z][A-Z\s.]+?)\s+(?:of|also)', re.IGNORECASE),
            ],
            'party_testator': [
                re.compile(r'TESTATOR[:\s]+([A-Z][A-Z\s.]+?)\s+\(', re.IGNORECASE),
                re.compile(r'I\s+([A-Z][A-Z\s.]+?)\s+\(holder.*?\).*?being of sound mind', re.IGNORECASE),
            ],
            'party_notary': [
                re.compile(r'I,\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s+of.*?Notary Public', re.IGNORECASE),
            ],
            
            # Property patterns - Enhanced
            'plan_no': [
                re.compile(r'Plan\s+No\.?\s*([0-9]+)', re.IGNORECASE),
                re.compile(r'plan\s+No\.?\s*([0-9]+)', re.IGNORECASE),
            ],
            'plan_date': [
                re.compile(r'Plan.*?dated\s+([0-9]{4}\.[0-9]{2}\.[0-9]{2})', re.IGNORECASE),
                re.compile(r'plan.*?dated\s+([0-9]{2}\.[0-9]{2}\.[0-9]{4})', re.IGNORECASE),
            ],
            'lot': [
                re.compile(r'(?:Lot|lot)\s+([0-9A-Z]+)\b(?:\s+in\s+(?:Plan|plan)|,|\s+depicted)'),
                re.compile(r'marked\s+(?:Lot|lot)\s+([0-9A-Z]+)', re.IGNORECASE),
            ],
            'assessment_no': [
                re.compile(r'Assessment\s+No\.?\s*([0-9/A-Z-]+)', re.IGNORECASE),
                re.compile(r'bearing\s+(?:assessment|Assessment)\s+No\.?\s*([0-9/A-Z-]+)', re.IGNORECASE),
            ],
            
            # Administrative - Multiple sources
            'registry': [
                re.compile(r'Land\s+Registry\s+(?:at|office)\s+([A-Z][a-z]+(?:-[A-Z][a-z]+)?)', re.IGNORECASE),
                re.compile(r'registered\s+at.*?registry.*?\s+([A-Z][a-z]+)', re.IGNORECASE),
            ],
            'registration_no': [
                re.compile(r'(?:registered under|title)\s+([A-Z]\s*[0-9]+/[0-9]+)', re.IGNORECASE),
                re.compile(r'(?:Folio|folio)\s+([A-Z]\s*[0-9]+/[0-9]+)', re.IGNORECASE),
            ],
            'district': [
                re.compile(r'District\s+of\s+([A-Z][a-z]+)', re.IGNORECASE),
                re.compile(r'in\s+the\s+([A-Z][a-z]+)\s+District', re.IGNORECASE),
            ],
            'province': [
                re.compile(r'(Western|Central|Southern|Northern|Eastern|North Western|North Central|Uva|Sabaragamuwa)\s+Province', re.IGNORECASE),
            ],
            
            # Legal - Comprehensive
            'deed_type': [
                re.compile(r'DEED\s+OF\s+(TRANSFER|GIFT|MORTGAGE)', re.IGNORECASE),
                re.compile(r'(LAST WILL|MORTGAGE BOND|LEASE AGREEMENT)', re.IGNORECASE),
            ],
            'date': [
                re.compile(r'([0-9]{4}\.[0-9]{2}\.[0-9]{2})'),
                re.compile(r'([0-9]{2}/[0-9]{2}/[0-9]{4})'),
                re.compile(r'([0-9]{1,2}(?:st|nd|rd|th)?\s+(?:day of\s+)?(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+(?:Two Thousand|20)[0-9]{2,4})', re.IGNORECASE),
            ],
            'amount': [
                re.compile(r'Rs\.?\s*([\d,]+(?:\.\d{1,2})?)', re.IGNORECASE),
                re.compile(r'Rupees\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+\(Rs\.?\s*([\d,]+)', re.IGNORECASE),
            ],
            'prior_deed': [
                re.compile(r'Deed\s+No\.?\s+([0-9]+)\s+dated', re.IGNORECASE),
                re.compile(r'under.*?Deed\s+No\.?\s+([0-9]+)', re.IGNORECASE),
            ],
            
            # Boundaries - Robust extraction
            'boundaries': re.compile(
                r'(?:NORTH|North).*?[:\-]\s*(.+?)(?=(?:EAST|East|SOUTH|South|WEST|West|and containing|$))',
                re.IGNORECASE | re.DOTALL
            ),
            
            # NIC numbers
            'nic': re.compile(r'\b\d{12}\b|\b\d{9}[VvXx]\b'),
            
            # Extent/Area
            'extent': [
                re.compile(r'containing.*?extent[:\s]+([A-Z0-9\s.:]+?)\s*(?:PERCHES|Perches|perches)', re.IGNORECASE),
                re.compile(r'\(A[0-9]+-?R[0-9]+-?P[0-9.]+\)', re.IGNORECASE),
            ],
        }
    
    def extract_multi_pattern(self, text: str, patterns: List[re.Pattern]) -> List[str]:
        """Extract using multiple patterns and return unique results."""
        results = []
        for pattern in patterns:
            matches = pattern.findall(text)
            if matches:
                # Handle tuple matches (multiple groups)
                if isinstance(matches[0], tuple):
                    for match in matches:
                        # Get the first non-empty group
                        for group in match:
                            if group and group.strip():
                                results.append(self.clean_text(group))
                                break
                else:
                    results.extend([self.clean_text(m) for m in matches if m.strip()])
        
        # Return unique values while preserving order
        seen = set()
        unique = []
        for item in results:
            if item not in seen:
                seen.add(item)
                unique.append(item)
        return unique
    
    def extract_boundaries(self, text: str) -> Dict[str, str]:
        """Extract all four boundaries with enhanced parsing."""
        boundaries = {}
        
        # Extract each direction separately
        for direction in ['NORTH', 'EAST', 'SOUTH', 'WEST']:
            pattern = re.compile(
                rf'{direction}\s*[:\-]\s*(.+?)(?=(?:NORTH|EAST|SOUTH|WEST|and containing|containing|$))',
                re.IGNORECASE | re.DOTALL
            )
            match = pattern.search(text)
            if match:
                boundary_text = self.clean_text(match.group(1))
                # Clean up
                boundary_text = re.sub(r'\s*(NORTH|EAST|SOUTH|WEST).*$', '', boundary_text, flags=re.IGNORECASE)
                boundary_text = boundary_text.rstrip(',').strip()
                if boundary_text:
                    boundaries[direction[0]] = boundary_text
        
        return boundaries
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        # Remove common artifacts
        text = re.sub(r'\s*\(hereinafter.*?\)', '', text, flags=re.IGNORECASE)
        return text
    
    def extract_with_spacy(self, text: str) -> Dict:
        """Extract entities using trained SpaCy model."""
        if not self.use_spacy:
            return {}
        
        doc = self.nlp(text)
        entities = defaultdict(list)
        
        for ent in doc.ents:
            entities[ent.label_].append(self.clean_text(ent.text))
        
        return dict(entities)
    
    def extract_with_rules(self, text: str) -> Dict:
        """Extract entities using enhanced multi-pattern regex."""
        entities = {}
        
        for key, patterns in self.patterns.items():
            if key == 'boundaries':
                entities['boundaries'] = self.extract_boundaries(text)
            elif isinstance(patterns, list):
                results = self.extract_multi_pattern(text, patterns)
                if results:
                    entities[key] = results
            else:
                matches = patterns.findall(text)
                if matches:
                    entities[key] = [self.clean_text(m) if isinstance(m, str) else self.clean_text(m[0]) 
                                    for m in matches if m]
        
        return entities
    
    def merge_extractions(self, spacy_entities: Dict, rule_entities: Dict) -> Dict:
        """Intelligently merge SpaCy and rule-based extractions."""
        merged = {}
        
        # Helper to get first value from list
        def first_or_none(items):
            return items[0] if items else None
        
        # Parties - prefer rules, enhance with SpaCy
        for party_type in ['vendor', 'vendee', 'donor', 'donee', 'testator']:
            names = []
            
            # Try rule-based first
            rule_key = f'party_{party_type}'
            if rule_key in rule_entities:
                names = rule_entities[rule_key]
            
            # Enhance with SpaCy if available
            spacy_key = f'PARTY_{party_type.upper()}'
            if not names and spacy_key in spacy_entities:
                names = spacy_entities[spacy_key]
            
            if names:
                merged[party_type] = {"names": names}
        
        # Notary
        if 'party_notary' in rule_entities:
            merged['notary'] = {"name": first_or_none(rule_entities['party_notary'])}
        
        # Property - prefer rules
        merged['plan'] = {
            'plan_no': first_or_none(rule_entities.get('plan_no', [])),
            'plan_date': first_or_none(rule_entities.get('plan_date', [])),
            'surveyor': None
        }
        
        merged['property'] = {
            'lot': first_or_none(rule_entities.get('lot', [])),
            'assessment_no': first_or_none(rule_entities.get('assessment_no', [])),
            'extent': first_or_none(rule_entities.get('extent', [])),
            'boundaries': rule_entities.get('boundaries', {})
        }
        
        # Administrative
        merged['registry_office'] = first_or_none(rule_entities.get('registry', []))
        merged['registration_no'] = first_or_none(rule_entities.get('registration_no', []))
        merged['district'] = first_or_none(rule_entities.get('district', []))
        merged['province'] = first_or_none(rule_entities.get('province', []))
        merged['jurisdiction'] = merged['district']  # Use district as jurisdiction
        
        # Legal
        merged['deed_type'] = first_or_none(rule_entities.get('deed_type', []))
        merged['date'] = first_or_none(rule_entities.get('date', []))
        merged['prior_deed'] = first_or_none(rule_entities.get('prior_deed', []))
        
        # Amount - handle both formats
        if 'amount' in rule_entities and rule_entities['amount']:
            try:
                amount_str = rule_entities['amount'][0]
                # Remove commas and extract number
                amount_str = re.sub(r'[^\d.]', '', amount_str)
                merged['consideration_lkr'] = float(amount_str) if amount_str else None
            except:
                merged['consideration_lkr'] = None
        else:
            merged['consideration_lkr'] = None
        
        # NICs
        if 'nic' in rule_entities:
            merged['nics'] = list(set(rule_entities['nic']))  # Unique NICs
        
        return merged
    
    def calculate_quality_score(self, extracted: Dict) -> Dict:
        """Calculate comprehensive quality score."""
        score = 0
        max_score = 0
        issues = []
        warnings = []
        
        # Critical fields (3 points each)
        critical = [
            ('deed_type', 'Deed type'),
            ('date', 'Execution date'),
        ]
        
        for field, name in critical:
            max_score += 3
            if extracted.get(field):
                score += 3
            else:
                issues.append(f"Missing {name}")
        
        # Important fields (2 points each)
        important = [
            ('plan.plan_no', 'Plan number'),
            ('property.lot', 'Lot number'),
            ('registry_office', 'Registry office'),
        ]
        
        for field, name in important:
            max_score += 2
            value = self._get_nested(extracted, field)
            if value:
                score += 2
            else:
                warnings.append(f"Missing {name}")
        
        # Useful fields (1 point each)
        useful = [
            ('district', 'District'),
            ('property.assessment_no', 'Assessment number'),
            ('property.boundaries', 'Boundaries'),
            ('consideration_lkr', 'Consideration amount'),
        ]
        
        for field, name in useful:
            max_score += 1
            value = self._get_nested(extracted, field)
            if value:
                if field == 'property.boundaries':
                    filled = sum(1 for v in value.values() if v)
                    if filled >= 3:
                        score += 1
                    elif filled >= 1:
                        score += 0.5
                else:
                    score += 1
            else:
                warnings.append(f"Missing {name}")
        
        # Parties (2 points for having parties)
        max_score += 2
        has_parties = any(k in extracted for k in ['vendor', 'vendee', 'donor', 'donee', 'testator'])
        if has_parties:
            # Check if names are actually extracted
            party_names_found = False
            for k in ['vendor', 'vendee', 'donor', 'donee', 'testator']:
                if k in extracted and extracted[k].get('names'):
                    party_names_found = True
                    break
            
            if party_names_found:
                score += 2
            else:
                score += 1
                warnings.append("Party roles found but no names extracted")
        else:
            issues.append("No parties identified")
        
        # Calculate percentage
        quality_percent = (score / max_score * 100) if max_score > 0 else 0
        
        # Determine rating
        if quality_percent >= 85:
            rating = "EXCELLENT"
        elif quality_percent >= 70:
            rating = "GOOD"
        elif quality_percent >= 50:
            rating = "REVIEW"
        else:
            rating = "POOR"
        
        return {
            "score": round(score, 1),
            "max_score": max_score,
            "percentage": round(quality_percent, 1),
            "rating": rating,
            "issues": issues,
            "warnings": warnings,
            "needs_review": rating in ["REVIEW", "POOR"] or len(issues) > 0
        }
    
    def _get_nested(self, data: Dict, path: str):
        """Get nested dictionary value."""
        keys = path.split('.')
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    
    def extract_deed(self, text: str, deed_id: Optional[str] = None) -> Dict:
        """Main extraction method."""
        # Extract with both methods
        spacy_entities = self.extract_with_spacy(text) if self.use_spacy else {}
        rule_entities = self.extract_with_rules(text)
        
        # Merge results
        merged = self.merge_extractions(spacy_entities, rule_entities)
        
        # Determine deed type
        deed_type = 'unknown'
        if merged.get('deed_type'):
            dt = merged['deed_type'].lower()
            if 'transfer' in dt:
                deed_type = 'sale_transfer'
            elif 'gift' in dt:
                deed_type = 'gift'
            elif 'mortgage' in dt:
                deed_type = 'mortgage'
            elif 'will' in dt or 'testament' in dt:
                deed_type = 'will'
            elif 'lease' in dt:
                deed_type = 'lease'
        
        # Calculate quality
        quality = self.calculate_quality_score(merged)
        
        # Build result
        result = {
            "id": deed_id or str(uuid.uuid4()),
            "type": deed_type,
            "code_number": merged.get('registration_no') or f"UNKNOWN-{deed_id or uuid.uuid4().hex[:8]}",
            "date": merged.get('date'),
            "jurisdiction": merged.get('jurisdiction'),
            "district": merged.get('district'),
            "province": merged.get('province'),
            "registry_office": merged.get('registry_office'),
            "plan": merged.get('plan', {}),
            "property": merged.get('property', {}),
            "consideration_lkr": merged.get('consideration_lkr'),
            "prior_deed": merged.get('prior_deed'),
            "source": {
                "provenance": "improved_hybrid_extractor_v3",
                "extraction_method": "spacy+advanced_rules" if self.use_spacy else "advanced_rules",
                "quality_score": quality
            }
        }
        
        # Add party fields
        if deed_type == 'sale_transfer':
            result['vendor'] = merged.get('vendor', {"names": []})
            result['vendee'] = merged.get('vendee', {"names": []})
        elif deed_type == 'gift':
            result['donor'] = merged.get('donor', {"names": []})
            result['donee'] = merged.get('donee', {"names": []})
        elif deed_type == 'will':
            result['testator'] = merged.get('testator', {"names": []})
        
        # Add notary if present
        if merged.get('notary'):
            result['notary'] = merged['notary']
        
        # Add NICs
        if merged.get('nics'):
            result['ids'] = {"nic_all": merged['nics']}
        
        return result
    
    def process_folder(self, input_dir: str, output_dir: str) -> List[Dict]:
        """Process all deeds in a folder."""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Find all .txt files
        deed_files = list(input_path.glob("*.txt"))
        
        if not deed_files:
            print(f"❌ No .txt files found in: {input_path}")
            return []
        
        print("="*80)
        print("PROCESSING DEED FOLDER")
        print("="*80)
        print(f"Input:  {input_path}")
        print(f"Output: {output_path}")
        print(f"Files:  {len(deed_files)}")
        print("-"*80)
        
        results = []
        stats = {
            'total': len(deed_files),
            'processed': 0,
            'failed': 0,
            'quality': {'EXCELLENT': 0, 'GOOD': 0, 'REVIEW': 0, 'POOR': 0}
        }
        
        for i, file_path in enumerate(sorted(deed_files), 1):
            try:
                print(f"\n[{i}/{len(deed_files)}] {file_path.name}")
                
                # Read file
                text = file_path.read_text(encoding='utf-8', errors='ignore')
                
                # Extract
                deed_id = file_path.stem
                result = self.extract_deed(text, deed_id=deed_id)
                
                # Save
                output_file = output_path / f"{deed_id}.json"
                with output_file.open('w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                
                # Update stats
                quality = result['source']['quality_score']
                stats['processed'] += 1
                stats['quality'][quality['rating']] += 1
                
                # Display
                print(f"  Type: {result['type']}")
                print(f"  Quality: {quality['rating']} ({quality['percentage']}%)")
                if quality['issues']:
                    print(f"  Issues: {', '.join(quality['issues'][:3])}")
                if quality['needs_review']:
                    print(f"  ⚠ NEEDS REVIEW")
                
                results.append(result)
                
            except Exception as e:
                print(f"  ❌ Error: {e}")
                stats['failed'] += 1
        
        # Summary
        print("\n" + "="*80)
        print("PROCESSING SUMMARY")
        print("="*80)
        print(f"Total: {stats['total']}")
        print(f"Processed: {stats['processed']}")
        print(f"Failed: {stats['failed']}")
        print(f"\nQuality Distribution:")
        for rating in ['EXCELLENT', 'GOOD', 'REVIEW', 'POOR']:
            count = stats['quality'][rating]
            if count > 0:
                pct = (count / stats['processed'] * 100) if stats['processed'] > 0 else 0
                print(f"  {rating:10s}: {count:3d} ({pct:.1f}%)")
        
        # Save summary
        summary_file = output_path / '_processing_summary.json'
        with summary_file.open('w', encoding='utf-8') as f:
            json.dump({
                'statistics': stats,
                'files_processed': [r['id'] for r in results]
            }, f, indent=2)
        
        print(f"\n✓ Summary saved: {summary_file}")
        
        return results


# Main execution
if __name__ == "__main__":
    import sys
    
    print("="*80)
    print("IMPROVED HYBRID DEED EXTRACTOR")
    print("="*80)
    
    # Configuration
    SPACY_MODEL = "../model/deed_ner_model"  
    INPUT_DIR = "./deeds/unprocessed2"
    OUTPUT_DIR = "./deeds/processed2"
    
    # Command line args
    if len(sys.argv) > 1:
        INPUT_DIR = sys.argv[1]
    if len(sys.argv) > 2:
        OUTPUT_DIR = sys.argv[2]
    if len(sys.argv) > 3:
        SPACY_MODEL = sys.argv[3] if sys.argv[3] != "none" else None
    
    # Initialize extractor
    extractor = ImprovedHybridDeedExtractor(spacy_model_path=SPACY_MODEL)
    
    # Check if processing folder or single deed
    input_path = Path(INPUT_DIR)
    
    if input_path.is_dir():
        # Process entire folder
        print(f"\nProcessing folder: {input_path}")
        results = extractor.process_folder(INPUT_DIR, OUTPUT_DIR)
        
    elif input_path.is_file():
        # Process single file
        print(f"\nProcessing single file: {input_path}")
        text = input_path.read_text(encoding='utf-8', errors='ignore')
        result = extractor.extract_deed(text, deed_id=input_path.stem)
        
        # Save
        output_path = Path(OUTPUT_DIR)
        output_path.mkdir(parents=True, exist_ok=True)
        output_file = output_path / f"{input_path.stem}.json"
        
        with output_file.open('w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Saved to: {output_file}")
        print(f"\nQuality: {result['source']['quality_score']['rating']}")
        print(f"Score: {result['source']['quality_score']['percentage']}%")
        
    else:
        print(f"\n❌ Path not found: {input_path}")
        print("\nUsage:")
        print("  python improved_hybrid_extractor.py <input_folder> [output_folder] [spacy_model]")
        print("  python improved_hybrid_extractor.py ./deeds/unprocessed ./deeds/processed")
        print("  python improved_hybrid_extractor.py ./deeds/unprocessed ./deeds/processed none")