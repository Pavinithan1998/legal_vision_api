"""
LegalVision - Deed Knowledge Base Uploader
==========================================
Accepts one or many deed files (DOCX or TXT), runs the full pipeline:
    DOCX/TXT → extract text → NER extraction → JSON → Neo4j

All intermediate files are written to a temporary directory and deleted
when the upload is complete (success or failure).

Usage (standalone):
    python deed_uploader.py file1.docx file2.docx ...
    python deed_uploader.py ./folder_of_deeds/

Usage (as a module in your backend):
    from deed_uploader import DeedUploader

    uploader = DeedUploader()
    result = uploader.upload_files(["path/to/deed.docx", "path/to/deed2.txt"])
    print(result)

Environment variables required (.env or system):
    NEO4J_URI
    NEO4J_USER
    NEO4J_PASS

Optional:
    SPACY_MODEL_PATH   Path to trained SpaCy NER model (falls back to rule-only)
"""

import json
import os
import re
import shutil
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# Re-use existing project modules if available,
# otherwise the classes are inlined below so
# this file works as a standalone script too.
# ─────────────────────────────────────────────
try:
    from hybrid_deed_extractor import ImprovedHybridDeedExtractor
    _EXTRACTOR_IMPORTED = True
except ImportError:
    _EXTRACTOR_IMPORTED = False

try:
    from neo4j_loader_v2 import LegalKnowledgeGraphLoader, setup_schema
    _LOADER_IMPORTED = True
except ImportError:
    _LOADER_IMPORTED = False


# =============================================================================
# STEP 1 – DOCX → TXT
# =============================================================================

def _read_docx(file_path: Path) -> str:
    """Extract plain text from a DOCX file."""
    try:
        from docx import Document
    except ImportError:
        raise RuntimeError("python-docx is required: pip install python-docx")
    doc = Document(str(file_path))
    return "\n".join(p.text for p in doc.paragraphs)


def _clean_text(text: str) -> str:
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def _is_multi_deed(text: str) -> bool:
    pattern = r"(?:^|\n)\s*(?:\*\*)?(?:Deed|DEED|deed)\s+\d+\s*[-–—]"
    return len(re.findall(pattern, text, re.MULTILINE)) > 1


def _split_multi_deed(text: str) -> List[Tuple[str, str]]:
    """Return list of (deed_num, deed_text)."""
    pattern = r"(?:^|\n)\s*(?:\*\*)?(?:Deed|DEED|deed)\s+(\d+)\s*[-–—]\s*"
    matches = list(re.finditer(pattern, text, re.MULTILINE))
    deeds = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        deed_text = text[start:end].strip()
        if len(deed_text) >= 200:
            deeds.append((m.group(1), deed_text))
    return deeds


def extract_txt_files(
    source_files: List[Path],
    txt_dir: Path,
) -> List[Path]:
    """
    Convert every source file (DOCX or TXT) to one or more TXT files
    inside txt_dir.  Returns the list of created TXT paths.
    """
    txt_dir.mkdir(parents=True, exist_ok=True)
    created: List[Path] = []
    counter = 1

    for src in source_files:
        ext = src.suffix.lower()

        if ext == ".docx":
            raw = _read_docx(src)
        elif ext == ".txt":
            raw = src.read_text(encoding="utf-8", errors="ignore")
        else:
            print(f"  ⚠  Skipping unsupported file type: {src.name}")
            continue

        if _is_multi_deed(raw):
            deeds = _split_multi_deed(raw)
            for deed_num, deed_text in deeds:
                out = txt_dir / f"DEED_{counter:04d}.txt"
                out.write_text(_clean_text(deed_text), encoding="utf-8")
                created.append(out)
                counter += 1
        else:
            out = txt_dir / f"DEED_{counter:04d}_{src.stem}.txt"
            out.write_text(_clean_text(raw), encoding="utf-8")
            created.append(out)
            counter += 1

    return created


# =============================================================================
# STEP 2 – TXT → JSON  (inline extractor when import is unavailable)
# =============================================================================

def extract_json_files(
    txt_dir: Path,
    json_dir: Path,
    spacy_model_path: Optional[str] = None,
) -> List[Path]:
    """
    Run NER extraction on every TXT file and write JSON output.
    Returns list of created JSON paths.
    """
    json_dir.mkdir(parents=True, exist_ok=True)

    if _EXTRACTOR_IMPORTED:
        extractor = ImprovedHybridDeedExtractor(spacy_model_path=spacy_model_path)
    else:
        # ── Minimal inline fallback (rule-only, no SpaCy) ──────────────────
        class _MinimalExtractor:
            def extract_deed(self, text: str, deed_id: str = None) -> Dict:
                deed_id = deed_id or str(uuid.uuid4())
                deed_type = "unknown"
                for kw, t in [
                    ("TRANSFER", "sale_transfer"), ("SALE", "sale_transfer"),
                    ("GIFT", "gift"), ("MORTGAGE", "mortgage"),
                    ("WILL", "will"), ("LEASE", "lease"),
                ]:
                    if kw in text.upper():
                        deed_type = t
                        break

                reg_m = re.search(r"(?:No\.?|Number)\s*([A-Z0-9/\-]{4,20})", text)
                date_m = re.search(r"\b(\d{4}[.\-/]\d{2}[.\-/]\d{2})\b", text)
                dist_m = re.search(
                    r"\b(Colombo|Kandy|Galle|Jaffna|Kurunegala|Gampaha|Kalutara"
                    r"|Matara|Hambantota|Batticaloa|Trincomalee|Ampara"
                    r"|Anuradhapura|Polonnaruwa|Badulla|Ratnapura|Kegalle"
                    r"|Nuwara Eliya|Matale|Vavuniya|Mannar|Puttalam|Monaragala)\b",
                    text, re.IGNORECASE,
                )
                return {
                    "id": deed_id,
                    "type": deed_type,
                    "code_number": (reg_m.group(1) if reg_m
                                    else f"UNKNOWN-{deed_id[:8]}"),
                    "date": date_m.group(1) if date_m else None,
                    "district": dist_m.group(1).title() if dist_m else None,
                    "jurisdiction": dist_m.group(1).title() if dist_m else None,
                    "province": None,
                    "registry_office": None,
                    "plan": {},
                    "property": {},
                    "consideration_lkr": None,
                    "prior_deed": None,
                    "source": {
                        "provenance": "minimal_inline_extractor",
                        "extraction_method": "rule_only_fallback",
                        "quality_score": {
                            "score": 0,
                            "max_score": 10,
                            "percentage": 0.0,
                            "rating": "REVIEW",
                            "issues": ["Minimal extractor used – hybrid_deed_extractor.py not found"],
                            "warnings": [],
                            "needs_review": True,
                        },
                    },
                }
        extractor = _MinimalExtractor()

    created: List[Path] = []
    for txt_file in sorted(txt_dir.glob("*.txt")):
        text = txt_file.read_text(encoding="utf-8", errors="ignore")
        result = extractor.extract_deed(text, deed_id=txt_file.stem)
        out = json_dir / f"{txt_file.stem}.json"
        out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        created.append(out)

    return created


# =============================================================================
# STEP 3 – JSON → Neo4j  (inline loader when import is unavailable)
# =============================================================================

def load_json_to_neo4j(
    json_dir: Path,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_pass: str,
) -> Dict:
    """Push all JSON files from json_dir into Neo4j. Returns stats dict."""
    if _LOADER_IMPORTED:
        with LegalKnowledgeGraphLoader(neo4j_uri, neo4j_user, neo4j_pass) as loader:
            setup_schema(loader.driver)
            stats = loader.load_directory(json_dir)
        return stats

    # ── Inline loader when neo4j_loader_v2.py is not importable ──────────
    try:
        from neo4j import GraphDatabase
    except ImportError:
        raise RuntimeError("neo4j driver required: pip install neo4j")

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pass))
    stats = {"loaded": 0, "errors": 0}

    json_files = [f for f in sorted(json_dir.glob("*.json"))
                  if not f.name.startswith("_")]

    for f in json_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            code = data.get("code_number") or data.get("id", str(uuid.uuid4()))

            with driver.session() as session:
                session.run(
                    """
                    MERGE (i:Instrument {code_number: $code})
                    ON CREATE SET
                        i.id                = $id,
                        i.type              = $type,
                        i.date              = $date,
                        i.district          = $district,
                        i.province          = $province,
                        i.extraction_method = $extraction_method,
                        i.quality_rating    = $quality_rating,
                        i.needs_review      = $needs_review,
                        i.created_at        = datetime()
                    ON MATCH SET
                        i.type              = $type,
                        i.date              = $date,
                        i.district          = $district,
                        i.province          = $province,
                        i.extraction_method = $extraction_method,
                        i.quality_rating    = $quality_rating,
                        i.needs_review      = $needs_review,
                        i.updated_at        = datetime()
                    """,
                    code=code,
                    id=data.get("id"),
                    type=data.get("type"),
                    date=data.get("date"),
                    district=data.get("district"),
                    province=data.get("province"),
                    extraction_method=data.get("source", {}).get("extraction_method"),
                    quality_rating=data.get("source", {})
                                       .get("quality_score", {})
                                       .get("rating"),
                    needs_review=data.get("source", {})
                                     .get("quality_score", {})
                                     .get("needs_review", True),
                )
            stats["loaded"] += 1
            print(f"  ✓ {f.name} → Neo4j")
        except Exception as e:
            stats["errors"] += 1
            print(f"  ✗ {f.name} – {e}")

    driver.close()
    return stats


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

class DeedUploader:
    """
    Full pipeline: source files → Neo4j, with automatic temp-dir cleanup.

    Parameters
    ----------
    neo4j_uri, neo4j_user, neo4j_pass:
        Connection details. Defaults to env vars NEO4J_URI / NEO4J_USER / NEO4J_PASS.
    spacy_model_path:
        Optional path to trained SpaCy model.
        Defaults to env var SPACY_MODEL_PATH or None (rule-only extraction).
    """

    def __init__(
        self,
        neo4j_uri: Optional[str] = None,
        neo4j_user: Optional[str] = None,
        neo4j_pass: Optional[str] = None,
        spacy_model_path: Optional[str] = None,
    ):
        self.neo4j_uri = neo4j_uri or os.getenv("NEO4J_URI")
        self.neo4j_user = neo4j_user or os.getenv("NEO4J_USER", "neo4j")
        self.neo4j_pass = neo4j_pass or os.getenv("NEO4J_PASS")
        self.spacy_model_path = (
            spacy_model_path or os.getenv("SPACY_MODEL_PATH")
        )

        if not self.neo4j_uri:
            raise ValueError("NEO4J_URI is required (env var or constructor arg).")
        if not self.neo4j_pass:
            raise ValueError("NEO4J_PASS is required (env var or constructor arg).")

    # ------------------------------------------------------------------
    def upload_files(self, file_paths: List[str]) -> Dict:
        """
        Upload one or many deed files to the knowledge base.

        Parameters
        ----------
        file_paths : list of str or Path
            Paths to DOCX or TXT files to ingest.

        Returns
        -------
        dict with keys:
            success       (bool)
            files_received (int)
            txt_files_created (int)
            json_files_created (int)
            neo4j_stats   (dict)
            errors        (list of str)
        """
        sources = [Path(p) for p in file_paths]
        result = {
            "success": False,
            "files_received": len(sources),
            "txt_files_created": 0,
            "json_files_created": 0,
            "neo4j_stats": {},
            "errors": [],
        }

        # Validate inputs
        for src in sources:
            if not src.exists():
                result["errors"].append(f"File not found: {src}")
            elif src.suffix.lower() not in (".docx", ".txt"):
                result["errors"].append(
                    f"Unsupported format '{src.suffix}' for {src.name} "
                    "(only .docx and .txt are accepted)"
                )

        if result["errors"]:
            return result

        # Work inside a temp directory that is always cleaned up
        tmp = tempfile.mkdtemp(prefix="legalvision_upload_")
        txt_dir = Path(tmp) / "txt"
        json_dir = Path(tmp) / "json"

        try:
            print("\n" + "=" * 60)
            print("LEGALVISION – DEED UPLOADER")
            print("=" * 60)
            print(f"Files to process : {len(sources)}")

            # ── Step 1: extract text ──────────────────────────────────
            print("\n[1/3] Extracting text from source files …")
            txt_files = extract_txt_files(sources, txt_dir)
            result["txt_files_created"] = len(txt_files)
            print(f"  → {len(txt_files)} TXT file(s) produced")

            if not txt_files:
                result["errors"].append("No text could be extracted from the provided files.")
                return result

            # ── Step 2: NER extraction → JSON ─────────────────────────
            print("\n[2/3] Running NER extraction …")
            json_files = extract_json_files(txt_dir, json_dir, self.spacy_model_path)
            result["json_files_created"] = len(json_files)
            print(f"  → {len(json_files)} JSON file(s) produced")

            if not json_files:
                result["errors"].append("NER extraction produced no output.")
                return result

            # ── Step 3: load to Neo4j ─────────────────────────────────
            print("\n[3/3] Loading to Neo4j knowledge graph …")
            neo4j_stats = load_json_to_neo4j(
                json_dir,
                self.neo4j_uri,
                self.neo4j_user,
                self.neo4j_pass,
            )
            result["neo4j_stats"] = neo4j_stats
            print(f"  → Stats: {neo4j_stats}")

            result["success"] = True
            print("\n✓ Upload complete.")

        except Exception as exc:
            result["errors"].append(str(exc))
            print(f"\n✗ Upload failed: {exc}")

        finally:
            # ── Always delete temp files ──────────────────────────────
            shutil.rmtree(tmp, ignore_errors=True)
            print(f"  (Temporary files cleaned up)")

        return result

    # ------------------------------------------------------------------
    def upload_folder(self, folder_path: str) -> Dict:
        """
        Convenience wrapper: upload every DOCX/TXT inside a folder.

        Parameters
        ----------
        folder_path : str or Path
        """
        folder = Path(folder_path)
        if not folder.is_dir():
            return {
                "success": False,
                "errors": [f"Not a directory: {folder}"],
            }

        files = (
            list(folder.glob("*.docx"))
            + list(folder.glob("*.DOCX"))
            + list(folder.glob("*.txt"))
            + list(folder.glob("*.TXT"))
        )

        if not files:
            return {
                "success": False,
                "errors": [f"No .docx or .txt files found in: {folder}"],
            }

        return self.upload_files([str(f) for f in files])


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nExamples:")
        print("  python deed_uploader.py deed1.docx deed2.docx")
        print("  python deed_uploader.py ./deeds_folder/")
        sys.exit(1)

    uploader = DeedUploader()

    arg = Path(sys.argv[1])
    if arg.is_dir():
        result = uploader.upload_folder(str(arg))
    else:
        result = uploader.upload_files(sys.argv[1:])

    print("\nResult:")
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
