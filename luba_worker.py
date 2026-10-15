#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
luba_worker.py — Worker Python de Luba Engine (protocole JSON v1)
HIKLON TECHNOLOGIES

Un processus par tâche : Node envoie UNE requête JSON sur stdin, le worker répond par UN document
JSON sur stdout puis se termine. Aucun état n'est conservé (économie de mémoire sur Render Free).

Requête  : {"v":1,"id":"<id>","op":"<opération>","args":{...},"root":"/abs/espace/de/travail","limits":{...}}
Réponse  : {"v":1,"id":"<id>","status":"ok"|"error","result":{...},"error":{"code","message"},"verification":{...}}

Le worker ne lit/écrit QUE sous `root` (chemins relatifs, résolus avec realpath, liens symboliques refusés).
Il n'exécute JAMAIS de code trouvé dans les fichiers analysés (ast.parse / compile() seulement analysent).

Opérations : capabilities · analyze_file · syntax_check · zip_inspect · zip_extract · project_scan ·
             read_slice · diff_dirs · pack_zip · hash_tree · syntax_check_many
Compatible Python >= 3.8. Dépendances facultatives (détectées, jamais exigées) : voir requirements.txt.
"""
import ast
import difflib
import hashlib
import importlib
import json
import os
import re
import stat
import sys
import zipfile

PROTOCOL_VERSION = 1
WORKER_VERSION = "1.0.0"

DEFAULT_LIMITS = {
    "max_file_bytes": 10 * 1024 * 1024,        # taille max d'un fichier analysé
    "max_unpacked_bytes": 50 * 1024 * 1024,    # volume décompressé max d'une archive
    "max_files": 500,                          # nombre max d'entrées d'archive / de fichiers scannés
    "max_ratio": 100,                          # ratio de compression suspect
    "max_output_bytes": 256 * 1024,            # taille max de la réponse JSON
    "preview_chars": 4000,                     # extrait de contenu renvoyé
    "as_mb": 768,                              # RLIMIT_AS (si disponible)
    "cpu_s": 25,                               # RLIMIT_CPU (si disponible)
}

SKIP_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv", "dist", "build", ".idea", ".vscode", ".mypy_cache", ".pytest_cache"}

LANG_BY_EXT = {
    ".py": "python", ".js": "javascript", ".mjs": "javascript", ".cjs": "javascript", ".jsx": "javascript",
    ".ts": "typescript", ".tsx": "typescript", ".html": "html", ".htm": "html", ".css": "css",
    ".json": "json", ".xml": "xml", ".yaml": "yaml", ".yml": "yaml", ".c": "c", ".h": "c",
    ".cpp": "cpp", ".cc": "cpp", ".cxx": "cpp", ".hpp": "cpp", ".hh": "cpp", ".md": "markdown", ".txt": "text",
    ".csv": "csv", ".toml": "toml", ".ini": "ini", ".cfg": "ini", ".sh": "shell", ".sql": "sql",
    ".docx": "docx", ".pdf": "pdf", ".xlsx": "xlsx", ".png": "image", ".jpg": "image", ".jpeg": "image",
    ".gif": "image", ".webp": "image", ".zip": "zip",
}
TEXT_LANGS = {"python", "javascript", "typescript", "html", "css", "json", "xml", "yaml", "c", "cpp", "markdown",
              "text", "csv", "toml", "ini", "shell", "sql"}


class WorkerError(Exception):
    def __init__(self, code, message):
        Exception.__init__(self, message)
        self.code = code
        self.message = message


# ------------------------------------------------------------------------------------------------
# Utilitaires : limites, chemins, lecture
# ------------------------------------------------------------------------------------------------
def apply_resource_limits(limits):
    try:
        import resource
    except ImportError:  # Windows : on s'en remet au délai côté Node
        return
    def _set(which, soft):
        try:
            _, hard = resource.getrlimit(which)
            if hard != resource.RLIM_INFINITY:
                soft = min(soft, hard)
            resource.setrlimit(which, (soft, hard))
        except (ValueError, OSError):
            pass
    _set(resource.RLIMIT_CPU, int(limits["cpu_s"]))
    _set(resource.RLIMIT_AS, int(limits["as_mb"]) * 1024 * 1024)
    _set(resource.RLIMIT_FSIZE, int(limits["max_unpacked_bytes"]) + 1024 * 1024)
    try:
        _set(resource.RLIMIT_CORE, 0)
    except Exception:
        pass


def resolve_in_root(root, rel, must_exist=True, allow_root=False):
    """Résout `rel` sous `root`. Refuse : absolu, `..`, NUL, échappement par lien symbolique."""
    if not isinstance(rel, str) or "\x00" in rel:
        raise WorkerError("BAD_PATH", "chemin invalide")
    if rel in ("", "."):
        if allow_root:
            return os.path.realpath(root)
        raise WorkerError("BAD_PATH", "chemin vide")
    norm = rel.replace("\\", "/")
    if norm.startswith("/") or re.match(r"^[A-Za-z]:", norm):
        raise WorkerError("BAD_PATH", "chemin absolu refusé")
    if any(p == ".." for p in norm.split("/")):
        raise WorkerError("BAD_PATH", "traversée de répertoire refusée")
    real_root = os.path.realpath(root)
    full = os.path.join(real_root, *[p for p in norm.split("/") if p not in ("", ".")])
    real = os.path.realpath(full)
    if real != real_root and not real.startswith(real_root + os.sep):
        raise WorkerError("BAD_PATH", "le chemin sort de l'espace de travail")
    if must_exist and not os.path.exists(real):
        raise WorkerError("NOT_FOUND", "fichier introuvable : " + norm)
    if os.path.lexists(full) and os.path.islink(full):
        raise WorkerError("BAD_PATH", "lien symbolique refusé")
    return real


def relpath(root, full):
    return os.path.relpath(full, os.path.realpath(root)).replace(os.sep, "/")


def sha256_file(path, limit=None):
    h = hashlib.sha256()
    read = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            read += len(chunk)
            if limit is not None and read > limit:
                return None
            h.update(chunk)
    return h.hexdigest()


def detect_language(path):
    return LANG_BY_EXT.get(os.path.splitext(path)[1].lower(), "unknown")


def decode_bytes(data):
    """Retourne (texte|None, encodage, binaire?)."""
    if b"\x00" in data[:8192]:
        return None, None, True
    if data.startswith(b"\xef\xbb\xbf"):
        try:
            return data[3:].decode("utf-8"), "utf-8-sig", False
        except UnicodeDecodeError:
            pass
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return data.decode("utf-16"), "utf-16", False
        except UnicodeDecodeError:
            pass
    try:
        return data.decode("utf-8"), "utf-8", False
    except UnicodeDecodeError:
        return data.decode("latin-1"), "latin-1 (repli : UTF-8 invalide)", False


def read_text(root, rel, limits):
    full = resolve_in_root(root, rel)
    if not os.path.isfile(full):
        raise WorkerError("NOT_A_FILE", "n'est pas un fichier : " + rel)
    size = os.path.getsize(full)
    if size > limits["max_file_bytes"]:
        raise WorkerError("FILE_TOO_LARGE", "fichier trop volumineux (%d octets)" % size)
    with open(full, "rb") as f:
        data = f.read()
    text, enc, binary = decode_bytes(data)
    return full, data, text, enc, binary


def optional(name):
    try:
        return importlib.import_module(name)
    except Exception:
        return None


# ------------------------------------------------------------------------------------------------
# Contrôles syntaxiques (analyse statique uniquement — rien n'est exécuté)
# ------------------------------------------------------------------------------------------------
def check_python(text, filename):
    errors, warnings = [], []
    try:
        tree = ast.parse(text, filename=filename)
        compile(tree, filename, "exec", dont_inherit=True)
    except SyntaxError as e:
        errors.append({"line": e.lineno, "col": e.offset, "message": e.msg})
        return {"status": "failed", "tool": "python-ast+compile", "errors": errors, "warnings": warnings}
    except (ValueError, RecursionError, MemoryError) as e:
        errors.append({"line": None, "col": None, "message": "analyse impossible : " + str(e)[:200]})
        return {"status": "failed", "tool": "python-ast+compile", "errors": errors, "warnings": warnings}
    pyflakes_api = optional("pyflakes.api")
    tool = "python-ast+compile"
    if pyflakes_api is not None:
        try:
            import io
            reporter_mod = importlib.import_module("pyflakes.reporter")
            out, err = io.StringIO(), io.StringIO()
            pyflakes_api.check(text, filename, reporter_mod.Reporter(out, err))
            for line in out.getvalue().splitlines()[:30]:
                warnings.append({"line": None, "col": None, "message": line[:300]})
            tool += "+pyflakes"
        except Exception:
            pass
    return {"status": "passed", "tool": tool, "errors": errors, "warnings": warnings}


def check_json(text):
    try:
        json.loads(text)
        return {"status": "passed", "tool": "json", "errors": [], "warnings": []}
    except ValueError as e:
        line = getattr(e, "lineno", None)
        return {"status": "failed", "tool": "json", "errors": [{"line": line, "col": getattr(e, "colno", None), "message": str(e)[:300]}], "warnings": []}


def check_yaml(text):
    yaml = optional("yaml")
    if yaml is None:
        return {"status": "skipped", "tool": "yaml", "errors": [], "warnings": [], "detail": "PyYAML non installé"}
    try:
        list(yaml.safe_load_all(text))   # safe_load : aucune construction d'objet arbitraire
        return {"status": "passed", "tool": "pyyaml(safe)", "errors": [], "warnings": []}
    except Exception as e:
        mark = getattr(e, "problem_mark", None)
        return {"status": "failed", "tool": "pyyaml(safe)", "errors": [{"line": (mark.line + 1) if mark else None, "col": (mark.column + 1) if mark else None, "message": str(getattr(e, "problem", e))[:300]}], "warnings": []}


def check_xml(text):
    # DOCTYPE/ENTITY refusés : protège contre les expansions d'entités (« billion laughs », XXE).
    if re.search(r"<!\s*(DOCTYPE|ENTITY)", text, re.I):
        return {"status": "failed", "tool": "xml", "errors": [{"line": None, "col": None, "message": "DOCTYPE/ENTITY refusés par prudence (XXE / expansion d'entités)"}], "warnings": []}
    from xml.etree import ElementTree
    try:
        ElementTree.fromstring(text.encode("utf-8"))
        return {"status": "passed", "tool": "xml.etree", "errors": [], "warnings": []}
    except ElementTree.ParseError as e:
        pos = getattr(e, "position", (None, None))
        return {"status": "failed", "tool": "xml.etree", "errors": [{"line": pos[0], "col": pos[1], "message": str(e)[:300]}], "warnings": []}


VOID_TAGS = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}


def check_html(text):
    from html.parser import HTMLParser
    problems, warnings = [], []
    stack = []
    ids = {}

    class P(HTMLParser):
        def handle_starttag(self, tag, attrs):
            if tag in VOID_TAGS:
                return
            stack.append((tag, self.getpos()[0]))
            for k, v in attrs:
                if k == "id" and v:
                    if v in ids:
                        warnings.append({"line": self.getpos()[0], "col": None, "message": "id dupliqué : " + v})
                    ids[v] = True

        def handle_startendtag(self, tag, attrs):
            return

        def handle_endtag(self, tag):
            if tag in VOID_TAGS:
                return
            if stack and stack[-1][0] == tag:
                stack.pop()
                return
            names = [t for t, _ in stack]
            if tag in names:   # des balises ouvertes entre-temps n'ont pas été fermées
                while stack and stack[-1][0] != tag:
                    t, ln = stack.pop()
                    problems.append({"line": ln, "col": None, "message": "balise <%s> non fermée" % t})
                if stack:
                    stack.pop()
            else:
                problems.append({"line": self.getpos()[0], "col": None, "message": "balise fermante </%s> sans ouverture" % tag})

    p = P(convert_charrefs=True)
    try:
        p.feed(text)
        p.close()
    except Exception as e:
        problems.append({"line": None, "col": None, "message": "analyse HTML impossible : " + str(e)[:200]})
    for t, ln in stack:
        if t not in ("html", "body", "head", "p", "li"):   # fermetures facultatives en HTML5
            problems.append({"line": ln, "col": None, "message": "balise <%s> non fermée" % t})
    low = text.lower()
    if "<!doctype" not in low[:200]:
        warnings.append({"line": None, "col": None, "message": "DOCTYPE absent"})
    if "<title" not in low:
        warnings.append({"line": None, "col": None, "message": "<title> absent"})
    return {"status": "failed" if problems else "passed", "tool": "html.parser (structure uniquement)", "errors": problems[:50], "warnings": warnings[:30],
            "detail": "contrôle de structure : ne prouve pas que la page fonctionne"}


def check_css(text):
    """Contrôle limité : équilibre des accolades/parenthèses, chaînes et commentaires non fermés."""
    errors = []
    i, n, line = 0, len(text), 1
    brace = paren = 0
    brace_line = []
    while i < n:
        c = text[i]
        if c == "\n":
            line += 1
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            if j < 0:
                errors.append({"line": line, "col": None, "message": "commentaire non fermé"})
                break
            line += text.count("\n", i, j)
            i = j + 1
        elif c in "\"'":
            q, j = c, i + 1
            while j < n and text[j] != q:
                if text[j] == "\\":
                    j += 1
                if j < n and text[j] == "\n":
                    break
                j += 1
            if j >= n or text[j] != q:
                errors.append({"line": line, "col": None, "message": "chaîne non fermée"})
                break
            i = j
        elif c == "{":
            brace += 1
            brace_line.append(line)
        elif c == "}":
            brace -= 1
            if brace < 0:
                errors.append({"line": line, "col": None, "message": "« } » sans « { »"})
                brace = 0
        elif c == "(":
            paren += 1
        elif c == ")":
            paren -= 1
            if paren < 0:
                errors.append({"line": line, "col": None, "message": "« ) » sans « ( »"})
                paren = 0
        i += 1
    if brace > 0:
        errors.append({"line": brace_line[-1] if brace_line else None, "col": None, "message": "%d accolade(s) non fermée(s)" % brace})
    if paren > 0:
        errors.append({"line": None, "col": None, "message": "%d parenthèse(s) non fermée(s)" % paren})
    return {"status": "failed" if errors else "passed", "tool": "css-balance (limité)", "errors": errors[:30], "warnings": [],
            "detail": "contrôle limité (équilibre des délimiteurs) : pas de validation des propriétés"}


def syntax_check_text(text, language, filename):
    if language == "python":
        return check_python(text, filename)
    if language == "json":
        return check_json(text)
    if language == "yaml":
        return check_yaml(text)
    if language == "xml":
        return check_xml(text)
    if language == "html":
        return check_html(text)
    if language == "css":
        return check_css(text)
    return {"status": "skipped", "tool": None, "errors": [], "warnings": [], "detail": "aucun contrôle Python pour « %s »" % language}


# ------------------------------------------------------------------------------------------------
# Analyse de fichiers
# ------------------------------------------------------------------------------------------------
def py_symbols(text):
    try:
        tree = ast.parse(text)
    except Exception:
        return None
    imports, funcs, classes = [], [], []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(("." * (node.level or 0)) + (node.module or ""))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            funcs.append(node.name)
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)
    return {"imports": sorted(set(imports))[:60], "functions": funcs[:80], "classes": classes[:40]}


JS_IMPORT_RE = re.compile(r"""(?:require\(\s*['"]([^'"]+)['"]\s*\)|import\s+(?:[^'"]*?\s+from\s+)?['"]([^'"]+)['"])""")
C_INCLUDE_RE = re.compile(r'^\s*#\s*include\s*([<"])([^>"]+)[>"]', re.M)


def code_symbols(text, language):
    if language == "python":
        return py_symbols(text)
    if language in ("javascript", "typescript"):
        mods = sorted(set(a or b for a, b in JS_IMPORT_RE.findall(text)))[:60]
        funcs = re.findall(r"(?:function\s+([A-Za-z_$][\w$]*)|const\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s*)?\(?[^=]*=>)", text)
        return {"imports": mods, "functions": sorted(set(a or b for a, b in funcs))[:80], "classes": re.findall(r"class\s+([A-Za-z_$][\w$]*)", text)[:40]}
    if language in ("c", "cpp"):
        incs = ["%s%s" % ("<" if k == "<" else '"', v) for k, v in C_INCLUDE_RE.findall(text)]
        return {"imports": sorted(set(incs))[:60], "functions": [], "classes": re.findall(r"\b(?:class|struct)\s+([A-Za-z_]\w*)", text)[:40]}
    return None


def png_dims(data):
    if data[:8] == b"\x89PNG\r\n\x1a\n" and len(data) >= 24:
        return int.from_bytes(data[16:20], "big"), int.from_bytes(data[20:24], "big")
    return None


def jpeg_dims(data):
    if data[:2] != b"\xff\xd8":
        return None
    i = 2
    while i + 9 < len(data):
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        if marker in (0xC0, 0xC1, 0xC2):
            return int.from_bytes(data[i + 7:i + 9], "big"), int.from_bytes(data[i + 5:i + 7], "big")
        seg = int.from_bytes(data[i + 2:i + 4], "big")
        i += 2 + seg
    return None


def analyze_document(full, language, limits, notes):
    """Extraction d'aperçu pour DOCX / PDF / XLSX / CSV. Retourne dict ou None si la bibliothèque manque."""
    cap = int(limits["preview_chars"])
    if language == "docx":
        docx = optional("docx")
        if docx is None:
            notes.append("python-docx absent : DOCX non lisible")
            return None
        d = docx.Document(full)
        paras = [p.text for p in d.paragraphs if p.text.strip()]
        return {"paragraphs": len(paras), "tables": len(d.tables), "preview": "\n".join(paras)[:cap]}
    if language == "pdf":
        pypdf = optional("pypdf")
        if pypdf is None:
            notes.append("pypdf absent : PDF non lisible")
            return None
        r = pypdf.PdfReader(full)
        if getattr(r, "is_encrypted", False):
            notes.append("PDF chiffré : contenu non extrait")
            return {"pages": len(r.pages), "encrypted": True, "preview": ""}
        out = []
        for page in r.pages[:20]:
            out.append(page.extract_text() or "")
            if sum(len(x) for x in out) > cap:
                break
        text = "\n".join(out)
        if not text.strip():
            notes.append("aucun texte extractible (PDF scanné ? l'OCR n'est pas implémenté)")
        return {"pages": len(r.pages), "preview": text[:cap]}
    if language == "xlsx":
        openpyxl = optional("openpyxl")
        if openpyxl is None:
            notes.append("openpyxl absent : XLSX non lisible")
            return None
        wb = openpyxl.load_workbook(full, read_only=True, data_only=True)   # read_only : ne charge pas tout en mémoire
        sheets = []
        for ws in wb.worksheets[:10]:
            rows = []
            for row in ws.iter_rows(min_row=1, max_row=5, values_only=True):
                rows.append([("" if v is None else str(v))[:60] for v in row[:12]])
            sheets.append({"name": ws.title, "max_row": ws.max_row, "max_column": ws.max_column, "head": rows})
        wb.close()
        return {"sheets": sheets, "note": "dimensions annoncées par le classeur (mode lecture seule)"}
    return None


def op_analyze_file(root, args, limits):
    rel = args.get("path")
    full = resolve_in_root(root, rel)
    if not os.path.isfile(full):
        raise WorkerError("NOT_A_FILE", "n'est pas un fichier : " + str(rel))
    size = os.path.getsize(full)
    language = detect_language(full)
    out = {"path": relpath(root, full), "size": size, "language": language, "notes": []}
    if size == 0:
        out.update({"kind": "empty", "sha256": hashlib.sha256(b"").hexdigest()})
        out["notes"].append("fichier vide")
        return out, {"checked": True, "checks": []}
    if size > limits["max_file_bytes"]:
        out["kind"] = "too_large"
        out["notes"].append("fichier trop volumineux pour l'analyse (limite %d octets)" % limits["max_file_bytes"])
        return out, {"checked": False, "checks": []}
    out["sha256"] = sha256_file(full)
    with open(full, "rb") as f:
        head = f.read(16)
    # Le contenu réel prime sur l'extension : signatures reconnues.
    if head.startswith(b"PK\x03\x04") and language not in ("docx", "xlsx", "zip"):
        out["notes"].append("signature ZIP malgré l'extension « %s »" % os.path.splitext(full)[1])
        language = "zip"
        out["language"] = language
    if language == "zip":
        info, ver = op_zip_inspect(root, {"path": rel}, limits)
        out["kind"] = "archive"
        out["archive"] = info
        return out, ver
    if language in ("docx", "xlsx", "pdf"):
        out["kind"] = "document"
        try:
            doc = analyze_document(full, language, limits, out["notes"])
        except Exception as e:
            doc = None
            out["notes"].append("lecture impossible (fichier invalide ou corrompu ?) : " + str(e)[:200])
            out["valid"] = False
        if doc is not None:
            out["document"] = doc
            out["valid"] = True
        return out, {"checked": doc is not None, "checks": []}
    if language == "image":
        with open(full, "rb") as f:
            data = f.read(min(size, 2 * 1024 * 1024))
        dims = png_dims(data) or jpeg_dims(data)
        out["kind"] = "image"
        pil = optional("PIL.Image")
        if pil is not None:
            try:
                with pil.open(full) as im:
                    out["image"] = {"format": im.format, "width": im.size[0], "height": im.size[1], "mode": im.mode}
                    out["valid"] = True
            except Exception as e:
                out["valid"] = False
                out["notes"].append("image invalide ou format non pris en charge : " + str(e)[:160])
        elif dims:
            out["image"] = {"width": dims[0], "height": dims[1]}
            out["valid"] = True
            out["notes"].append("Pillow absent : dimensions lues dans l'en-tête (PNG/JPEG seulement)")
        else:
            out["notes"].append("format d'image non pris en charge sans Pillow")
        return out, {"checked": "image" in out, "checks": []}

    _, data, text, enc, binary = read_text(root, rel, limits)
    if binary:
        out["kind"] = "binary"
        out["notes"].append("fichier binaire : contenu non analysé")
        return out, {"checked": False, "checks": []}
    out["kind"] = "text"
    out["encoding"] = enc
    out["lines"] = text.count("\n") + (0 if text.endswith("\n") or not text else 1)
    out["preview"] = text[:int(limits["preview_chars"])]
    out["preview_truncated"] = len(text) > int(limits["preview_chars"])
    if "\r\n" in text:
        out["notes"].append("fins de ligne CRLF")
    sym = code_symbols(text, language)
    if sym:
        out["symbols"] = sym
    if language == "csv":
        import csv
        import io
        try:
            sample = text[:200000]
            try:
                dialect = csv.Sniffer().sniff(sample[:4096]) if sample.strip() else csv.excel
            except csv.Error:
                dialect = csv.excel   # séparateur indéterminable : on retombe sur la virgule
            rows = list(csv.reader(io.StringIO(sample), dialect))
            widths = set(len(r) for r in rows[:500] if r)
            out["csv"] = {"columns": len(rows[0]) if rows else 0, "rows_sampled": len(rows), "header": rows[0][:20] if rows else [], "ragged": len(widths) > 1}
        except Exception as e:
            out["notes"].append("CSV non analysable : " + str(e)[:160])
    chk = syntax_check_text(text, language, os.path.basename(full))
    return out, {"checked": chk["status"] != "skipped", "checks": [dict(chk, file=out["path"])]}


def op_syntax_check(root, args, limits):
    rel = args.get("path")
    _, _, text, _, binary = read_text(root, rel, limits)
    if binary:
        raise WorkerError("BINARY_FILE", "fichier binaire : contrôle impossible")
    language = args.get("language") or detect_language(rel)
    chk = syntax_check_text(text, language, os.path.basename(rel))
    chk["file"] = rel
    chk["language"] = language
    return chk, {"checked": chk["status"] != "skipped", "checks": [chk]}


def op_syntax_check_many(root, args, limits):
    """Contrôle syntaxique par lot (un seul processus pour N fichiers : économie sur Render Free)."""
    paths = args.get("paths")
    if not isinstance(paths, list):
        raise WorkerError("BAD_REQUEST", "paths doit être une liste")
    results = []
    for rel in paths[:200]:
        try:
            res, _ = op_syntax_check(root, {"path": rel}, limits)
        except WorkerError as e:
            res = {"file": rel, "status": "error", "tool": None, "errors": [{"line": None, "col": None, "message": e.message}], "warnings": []}
        results.append(res)
    return {"results": results}, {"checked": True, "checks": results}


def op_read_slice(root, args, limits):
    rel = args.get("path")
    _, _, text, enc, binary = read_text(root, rel, limits)
    if binary:
        raise WorkerError("BINARY_FILE", "fichier binaire")
    lines = text.splitlines()
    start = max(1, int(args.get("start_line") or 1))
    end = min(len(lines), int(args.get("end_line") or (start + 199)))
    if end - start > 1500:
        end = start + 1500
    chunk = "\n".join(lines[start - 1:end])
    return {"path": rel, "start_line": start, "end_line": end, "total_lines": len(lines), "content": chunk[:60000], "encoding": enc}, None


# ------------------------------------------------------------------------------------------------
# ZIP : inspection et extraction contrôlées
# ------------------------------------------------------------------------------------------------
def zip_entry_issues(info, seen, limits):
    issues = []
    raw = info.filename
    name = raw.replace("\\", "/")
    if "\x00" in raw:
        issues.append("octet NUL dans le nom")
    if name.startswith("/") or re.match(r"^[A-Za-z]:", name):
        issues.append("chemin absolu")
    if any(p == ".." for p in name.split("/")):
        issues.append("traversée de répertoire (..)")
    mode = (info.external_attr >> 16) & 0o170000
    if mode == stat.S_IFLNK:
        issues.append("lien symbolique")
    elif mode in (stat.S_IFCHR, stat.S_IFBLK, stat.S_IFIFO, stat.S_IFSOCK):
        issues.append("fichier spécial (périphérique/pipe/socket)")
    if info.flag_bits & 0x1:
        issues.append("entrée chiffrée")
    key = name.rstrip("/").lower()
    if key in seen:
        issues.append("nom dupliqué (insensible à la casse)")
    seen.add(key)
    if info.compress_size > 0 and info.file_size > 1024 * 1024 and info.file_size / float(info.compress_size) > limits["max_ratio"]:
        issues.append("ratio de compression suspect (%dx)" % (info.file_size // max(1, info.compress_size)))
    return name, issues


def op_zip_inspect(root, args, limits):
    full = resolve_in_root(root, args.get("path"))
    if os.path.getsize(full) > limits["max_unpacked_bytes"]:
        raise WorkerError("FILE_TOO_LARGE", "archive trop volumineuse")
    try:
        zf = zipfile.ZipFile(full)
    except (zipfile.BadZipFile, OSError) as e:
        raise WorkerError("ZIP_CORRUPT", "archive ZIP invalide ou corrompue : " + str(e)[:160])
    with zf:
        infos = zf.infolist()
        entries, problems = [], []
        seen = set()
        total = 0
        for info in infos[:limits["max_files"] + 1]:
            name, issues = zip_entry_issues(info, seen, limits)
            total += info.file_size
            if issues:
                problems.append({"name": name[:200], "issues": issues})
            if len(entries) < 200:
                entries.append({"name": name[:200], "size": info.file_size, "compressed": info.compress_size, "dir": info.is_dir()})
        if len(infos) > limits["max_files"]:
            problems.append({"name": "(archive)", "issues": ["trop d'entrées (%d > %d)" % (len(infos), limits["max_files"])]})
        total = sum(i.file_size for i in infos)
        if total > limits["max_unpacked_bytes"]:
            problems.append({"name": "(archive)", "issues": ["volume décompressé annoncé trop grand (%d octets)" % total]})
        corrupt = None
        try:
            corrupt = zf.testzip() if total <= limits["max_unpacked_bytes"] and len(infos) <= limits["max_files"] else None
        except Exception as e:
            corrupt = str(e)[:100]
        if corrupt:
            problems.append({"name": str(corrupt)[:200], "issues": ["entrée corrompue (CRC)"]})
        nested = [e["name"] for e in entries if e["name"].lower().endswith((".zip", ".tar", ".gz", ".7z", ".rar"))]
        scripts = [e["name"] for e in entries if e["name"].lower().endswith((".sh", ".bat", ".ps1", ".exe", ".dll", ".so"))]
    safe = not problems
    res = {"path": relpath(root, full), "entries": len(infos), "unpacked_bytes": total, "listing": entries, "listing_truncated": len(infos) > len(entries),
           "problems": problems[:50], "safe_to_extract": safe, "nested_archives_not_extracted": nested[:20],
           "executables_or_scripts_present_never_run": scripts[:20]}
    return res, {"checked": True, "checks": [{"file": res["path"], "status": "passed" if safe else "failed", "tool": "zipfile(inspect)", "errors": [{"line": None, "col": None, "message": "%s : %s" % (p["name"], ", ".join(p["issues"]))} for p in problems[:20]], "warnings": []}]}


def _rm_tree(path):
    for dirpath, dirnames, filenames in os.walk(path, topdown=False):
        for f in filenames:
            try:
                os.unlink(os.path.join(dirpath, f))
            except OSError:
                pass
        for d in dirnames:
            p = os.path.join(dirpath, d)
            try:
                os.rmdir(p) if not os.path.islink(p) else os.unlink(p)
            except OSError:
                pass
    try:
        os.rmdir(path)
    except OSError:
        pass


def op_zip_extract(root, args, limits):
    src = resolve_in_root(root, args.get("path"))
    dest_rel = args.get("dest")
    dest = resolve_in_root(root, dest_rel, must_exist=False)
    if os.path.exists(dest) and os.listdir(dest):
        raise WorkerError("DEST_NOT_EMPTY", "le dossier de destination n'est pas vide")
    info, ver = op_zip_inspect(root, {"path": args.get("path")}, limits)
    if not info["safe_to_extract"]:
        raise WorkerError("ZIP_UNSAFE", "archive refusée : " + "; ".join("%s (%s)" % (p["name"], ", ".join(p["issues"])) for p in info["problems"][:5]))
    os.makedirs(dest, exist_ok=True)
    written_files, written_bytes = 0, 0
    budget = limits["max_unpacked_bytes"]
    try:
        with zipfile.ZipFile(src) as zf:
            for zi in zf.infolist():
                name = zi.filename.replace("\\", "/")
                target = os.path.realpath(os.path.join(dest, *[p for p in name.split("/") if p not in ("", ".")]))
                if target != dest and not target.startswith(dest + os.sep):
                    raise WorkerError("ZIP_UNSAFE", "entrée hors destination : " + name[:120])
                if zi.is_dir():
                    os.makedirs(target, exist_ok=True)
                    continue
                os.makedirs(os.path.dirname(target), exist_ok=True)
                fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o644)
                with os.fdopen(fd, "wb") as out, zf.open(zi) as inp:
                    while True:
                        chunk = inp.read(256 * 1024)
                        if not chunk:
                            break
                        written_bytes += len(chunk)
                        if written_bytes > budget:   # on ne se fie pas aux tailles annoncées : le flux réel est compté
                            raise WorkerError("ZIP_TOO_BIG", "volume décompressé réel supérieur à la limite")
                        out.write(chunk)
                written_files += 1
                if written_files > limits["max_files"]:
                    raise WorkerError("ZIP_TOO_MANY", "trop de fichiers")
    except WorkerError:
        _rm_tree(dest)
        raise
    except (zipfile.BadZipFile, OSError, RuntimeError) as e:
        _rm_tree(dest)
        raise WorkerError("ZIP_EXTRACT_FAILED", "extraction impossible : " + str(e)[:160])
    ok = written_files == len([i for i in info["listing"] if not i["dir"]]) or info["listing_truncated"]
    res = {"dest": relpath(root, dest), "files": written_files, "bytes": written_bytes}
    return res, {"checked": True, "checks": [{"file": res["dest"], "status": "passed" if ok else "failed", "tool": "zip_extract(count)", "errors": [] if ok else [{"line": None, "col": None, "message": "nombre de fichiers extraits différent de l'inventaire"}], "warnings": []}]}


def op_pack_zip(root, args, limits):
    src = resolve_in_root(root, args.get("src"), allow_root=True)
    dest = resolve_in_root(root, args.get("dest"), must_exist=False)
    if os.path.exists(dest):
        raise WorkerError("DEST_EXISTS", "l'archive de destination existe déjà")
    count, total = 0, 0
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        for dirpath, dirnames, filenames in os.walk(src, followlinks=False):
            dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS and not os.path.islink(os.path.join(dirpath, d)))
            for f in sorted(filenames):
                p = os.path.join(dirpath, f)
                if os.path.islink(p) or not os.path.isfile(p):
                    continue
                total += os.path.getsize(p)
                count += 1
                if count > limits["max_files"] or total > limits["max_unpacked_bytes"]:
                    raise WorkerError("PACK_TOO_BIG", "trop de fichiers ou volume trop grand pour l'archive")
                zf.write(p, os.path.relpath(p, src).replace(os.sep, "/"))
    return {"path": relpath(root, dest), "files": count, "bytes": total, "size": os.path.getsize(dest), "sha256": sha256_file(dest)}, None


# ------------------------------------------------------------------------------------------------
# Projet : inventaire, relations, tests
# ------------------------------------------------------------------------------------------------
def walk_files(base, limits):
    out, skipped_links, truncated = [], 0, False
    for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
        keep = []
        for d in sorted(dirnames):
            if d in SKIP_DIRS:
                continue
            if os.path.islink(os.path.join(dirpath, d)):
                skipped_links += 1
                continue
            keep.append(d)
        dirnames[:] = keep
        for f in sorted(filenames):
            p = os.path.join(dirpath, f)
            if os.path.islink(p):
                skipped_links += 1
                continue
            out.append(p)
            if len(out) >= limits["max_files"]:
                return out, skipped_links, True
    return out, skipped_links, truncated


def op_hash_tree(root, args, limits):
    base = resolve_in_root(root, args.get("path"), allow_root=True)
    files, links, truncated = walk_files(base, dict(limits, max_files=limits["max_files"] * 4))
    manifest = {}
    for p in files:
        manifest[os.path.relpath(p, base).replace(os.sep, "/")] = sha256_file(p, limit=limits["max_unpacked_bytes"])
    return {"files": len(manifest), "manifest": manifest, "truncated": truncated}, None


def op_project_scan(root, args, limits):
    base = resolve_in_root(root, args.get("path"), allow_root=True)
    files, links, truncated = walk_files(base, limits)
    inv, by_rel = [], {}
    for p in files:
        rel = os.path.relpath(p, base).replace(os.sep, "/")
        lang = detect_language(p)
        size = os.path.getsize(p)
        entry = {"path": rel, "language": lang, "size": size}
        if lang in TEXT_LANGS and 0 < size <= 2 * 1024 * 1024:
            try:
                with open(p, "rb") as f:
                    data = f.read()
                text, _, binary = decode_bytes(data)
                if not binary:
                    entry["lines"] = text.count("\n") + 1
                    by_rel[rel] = text
            except OSError:
                pass
        inv.append(entry)
    names = set(e["path"] for e in inv)
    types, entry_points, notes = [], [], []
    if "package.json" in names:
        types.append("node")
        try:
            pj = json.loads(by_rel.get("package.json", "{}"))
            entry_points.append(pj.get("main") or "index.js")
            res_pkg = {"scripts": sorted((pj.get("scripts") or {}).keys())[:20], "dependencies": sorted((pj.get("dependencies") or {}).keys())[:60]}
        except ValueError:
            res_pkg = None
            notes.append("package.json invalide")
    else:
        res_pkg = None
    if names & {"requirements.txt", "pyproject.toml", "setup.py"} or any(e["language"] == "python" for e in inv):
        types.append("python")
    if names & {"CMakeLists.txt", "Makefile"} or any(e["language"] in ("c", "cpp") for e in inv):
        types.append("c/c++")
    if "index.html" in names or any(e["language"] == "html" for e in inv):
        types.append("web")
    for cand in ("main.py", "app.py", "index.js", "server.js", "main.c", "main.cpp", "index.html"):
        if cand in names:
            entry_points.append(cand)
    # Relations entre fichiers (imports locaux uniquement)
    relations = []
    pyfiles = {rel[:-3].replace("/", "."): rel for rel in names if rel.endswith(".py")}
    for rel, text in by_rel.items():
        lang = detect_language(rel)
        sym = code_symbols(text, lang) if lang in ("python", "javascript", "typescript", "c", "cpp") else None
        if not sym:
            continue
        for imp in sym["imports"]:
            target = None
            if lang == "python":
                modname = imp.lstrip(".")
                target = pyfiles.get(modname) or pyfiles.get(modname.split(".")[0])
            elif lang in ("javascript", "typescript") and imp.startswith("."):
                joined = os.path.normpath(os.path.join(os.path.dirname(rel), imp)).replace(os.sep, "/")
                for ext in ("", ".js", ".mjs", ".cjs", ".ts", ".json", "/index.js"):
                    if joined + ext in names:
                        target = joined + ext
                        break
            elif lang in ("c", "cpp") and imp.startswith('"'):
                inc = imp[1:]
                joined = os.path.normpath(os.path.join(os.path.dirname(rel), inc)).replace(os.sep, "/")
                target = joined if joined in names else (inc if inc in names else None)
            if target and target != rel:
                relations.append({"from": rel, "to": target})
    tests = [e["path"] for e in inv if re.search(r"(^|/)(test_[^/]*\.py|[^/]*_test\.py|[^/]*\.(test|spec)\.[cm]?[jt]s|tests?/[^/]*)$", e["path"])]
    langs = {}
    for e in inv:
        langs[e["language"]] = langs.get(e["language"], 0) + 1
    res = {"root": relpath(root, base) if base != os.path.realpath(root) else ".", "file_count": len(inv), "truncated": truncated, "skipped_symlinks": links,
           "project_types": types, "entry_points": entry_points[:6], "languages": langs, "package": res_pkg, "tests_found": tests[:30],
           "relations": relations[:300], "files": inv[:400], "files_listing_truncated": len(inv) > 400, "notes": notes}
    return res, None


def op_diff_dirs(root, args, limits):
    a = resolve_in_root(root, args.get("a"), allow_root=True)
    b = resolve_in_root(root, args.get("b"), allow_root=True)
    la, _, _ = walk_files(a, dict(limits, max_files=limits["max_files"] * 2))
    lb, _, _ = walk_files(b, dict(limits, max_files=limits["max_files"] * 2))
    ma = dict((os.path.relpath(p, a).replace(os.sep, "/"), p) for p in la)
    mb = dict((os.path.relpath(p, b).replace(os.sep, "/"), p) for p in lb)
    added = sorted(set(mb) - set(ma))
    removed = sorted(set(ma) - set(mb))
    changed, diffs, budget = [], [], 1500
    for rel in sorted(set(ma) & set(mb)):
        if sha256_file(ma[rel], limits["max_file_bytes"]) == sha256_file(mb[rel], limits["max_file_bytes"]):
            continue
        changed.append(rel)
    for rel in changed + added:
        src = ma.get(rel)
        dst = mb.get(rel)
        try:
            ta = decode_bytes(open(src, "rb").read(limits["max_file_bytes"]))[0] if src else ""
            tb = decode_bytes(open(dst, "rb").read(limits["max_file_bytes"]))[0] if dst else ""
        except OSError:
            continue
        if ta is None or tb is None:
            diffs.append({"path": rel, "binary": True})
            continue
        lines = list(difflib.unified_diff(ta.splitlines(), tb.splitlines(), "a/" + rel, "b/" + rel, lineterm="", n=3))
        cut = lines[:200]
        budget -= len(cut)
        diffs.append({"path": rel, "diff": "\n".join(cut), "truncated": len(lines) > len(cut), "added_lines": sum(1 for l in lines if l.startswith("+") and not l.startswith("+++")),
                      "removed_lines": sum(1 for l in lines if l.startswith("-") and not l.startswith("---"))})
        if budget <= 0:
            break
    return {"added": added[:200], "removed": removed[:200], "changed": changed[:200], "diffs": diffs, "identical": not (added or removed or changed)}, None


def op_capabilities(root, args, limits):
    mods = {}
    for label, mod in (("openpyxl", "openpyxl"), ("pandas", "pandas"), ("python-docx", "docx"), ("pypdf", "pypdf"), ("Pillow", "PIL"),
                       ("beautifulsoup4", "bs4"), ("lxml", "lxml"), ("pyyaml", "yaml"), ("pyflakes", "pyflakes")):
        mods[label] = optional(mod) is not None
    res_ok = True
    try:
        import resource  # noqa: F401
    except ImportError:
        res_ok = False
    formats = {
        "zip": {"supported": True, "level": "complet (inspection + extraction contrôlée)"},
        "text/code": {"supported": True, "level": "structure, encodage, symboles (Python/JS/TS/C/C++)"},
        "python": {"supported": True, "level": "syntaxe (ast+compile)" + (" + pyflakes" if mods["pyflakes"] else "")},
        "json": {"supported": True, "level": "syntaxe"},
        "xml": {"supported": True, "level": "syntaxe (DOCTYPE refusé)"},
        "html": {"supported": True, "level": "structure uniquement"},
        "css": {"supported": True, "level": "limité (équilibre des délimiteurs)"},
        "csv": {"supported": True, "level": "dimensions et cohérence des lignes"},
        "yaml": {"supported": mods["pyyaml"], "level": "syntaxe (safe_load)" if mods["pyyaml"] else "indisponible : PyYAML absent"},
        "docx": {"supported": mods["python-docx"], "level": "texte et tableaux (extraction)" if mods["python-docx"] else "indisponible : python-docx absent"},
        "pdf": {"supported": mods["pypdf"], "level": "texte extractible ; pas d'OCR" if mods["pypdf"] else "indisponible : pypdf absent"},
        "xlsx": {"supported": mods["openpyxl"], "level": "aperçu des feuilles (lecture seule)" if mods["openpyxl"] else "indisponible : openpyxl absent"},
        "image(png/jpeg)": {"supported": True, "level": "validation + dimensions (Pillow)" if mods["Pillow"] else "dimensions lues dans l'en-tête seulement"},
    }
    return {"worker_version": WORKER_VERSION, "protocol": PROTOCOL_VERSION, "python": sys.version.split()[0], "modules": mods,
            "resource_limits": res_ok, "formats": formats}, None


OPS = {
    "capabilities": op_capabilities, "analyze_file": op_analyze_file, "syntax_check": op_syntax_check, "zip_inspect": op_zip_inspect,
    "zip_extract": op_zip_extract, "project_scan": op_project_scan, "read_slice": op_read_slice, "diff_dirs": op_diff_dirs,
    "pack_zip": op_pack_zip, "hash_tree": op_hash_tree, "syntax_check_many": op_syntax_check_many,
}


def respond(real_out, payload, max_bytes):
    data = json.dumps(payload, ensure_ascii=False, default=str)
    if len(data.encode("utf-8")) > max_bytes:
        payload = {"v": PROTOCOL_VERSION, "id": payload.get("id"), "status": "error",
                   "error": {"code": "OUTPUT_TOO_LARGE", "message": "réponse du worker supérieure à %d octets" % max_bytes}}
        data = json.dumps(payload, ensure_ascii=False)
    real_out.write(data)
    real_out.flush()


def main():
    real_out = sys.stdout
    sys.stdout = sys.stderr            # tout print() parasite part sur stderr : stdout ne porte QUE la réponse JSON
    req_id = None
    max_out = DEFAULT_LIMITS["max_output_bytes"]
    try:
        raw = sys.stdin.read(4 * 1024 * 1024)
        req = json.loads(raw)
        if not isinstance(req, dict):
            raise WorkerError("BAD_REQUEST", "requête invalide")
        req_id = req.get("id")
        if req.get("v") != PROTOCOL_VERSION:
            raise WorkerError("PROTOCOL_MISMATCH", "version de protocole non prise en charge : %r (attendu %d)" % (req.get("v"), PROTOCOL_VERSION))
        limits = dict(DEFAULT_LIMITS)
        for k, v in (req.get("limits") or {}).items():
            if k in limits and isinstance(v, (int, float)) and v > 0:
                limits[k] = int(v)
        max_out = limits["max_output_bytes"]
        op = req.get("op")
        if op not in OPS:
            raise WorkerError("UNKNOWN_OP", "opération inconnue : %r" % (op,))
        root = req.get("root")
        if not isinstance(root, str) or not os.path.isabs(root) or not os.path.isdir(root):
            raise WorkerError("BAD_ROOT", "espace de travail invalide")
        args = req.get("args") if isinstance(req.get("args"), dict) else {}
        apply_resource_limits(limits)
        result, verification = OPS[op](root, args, limits)
        respond(real_out, {"v": PROTOCOL_VERSION, "id": req_id, "status": "ok", "result": result, "verification": verification}, max_out)
        return 0
    except WorkerError as e:
        respond(real_out, {"v": PROTOCOL_VERSION, "id": req_id, "status": "error", "error": {"code": e.code, "message": e.message}}, max_out)
        return 0
    except MemoryError:
        respond(real_out, {"v": PROTOCOL_VERSION, "id": req_id, "status": "error", "error": {"code": "OUT_OF_MEMORY", "message": "mémoire insuffisante"}}, max_out)
        return 0
    except json.JSONDecodeError as e:
        respond(real_out, {"v": PROTOCOL_VERSION, "id": req_id, "status": "error", "error": {"code": "BAD_REQUEST", "message": "JSON invalide : " + str(e)[:120]}}, max_out)
        return 0
    except Exception as e:   # jamais de trace brute vers l'extérieur : le type suffit, le détail reste sur stderr
        sys.stderr.write("worker exception: %r\n" % (e,))
        respond(real_out, {"v": PROTOCOL_VERSION, "id": req_id, "status": "error", "error": {"code": "WORKER_EXCEPTION", "message": "erreur interne du worker (%s)" % type(e).__name__}}, max_out)
        return 0


if __name__ == "__main__":
    sys.exit(main())
