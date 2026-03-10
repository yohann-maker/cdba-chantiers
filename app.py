#!/usr/bin/env python3
"""
CDBA — App Préparation Chantiers
Workflow : Sellsy → William (prépa) → Julien (commande) → Gina/Charlotte (planning) → Prêt
"""

import json
import os
import re
import hashlib
import uuid
import time
import logging
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, Form, HTTPException, Response, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from requests_oauthlib import OAuth1Session
import requests

logger = logging.getLogger("cdba")

# ──────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────

app = FastAPI(title="CDBA Chantiers")

BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
DATA_DIR.mkdir(exist_ok=True)
UPLOADS_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")

# Utilisateurs et mots de passe (via variables d'environnement)
USERS = {
    "william": {"name": "William", "role": "preparateur", "password": os.getenv("PASS_WILLIAM", "cdba2026")},
    "julien": {"name": "Julien", "role": "acheteur", "password": os.getenv("PASS_JULIEN", "cdba2026")},
    "gina": {"name": "Gina", "role": "planificatrice", "password": os.getenv("PASS_GINA", "cdba2026")},
    "charlotte": {"name": "Charlotte", "role": "planificatrice", "password": os.getenv("PASS_CHARLOTTE", "cdba2026")},
    "yohann": {"name": "Yohann", "role": "admin", "password": os.getenv("PASS_YOHANN", "cdba2026")},
}

# Sellsy API config (via variables d'environnement)
SELLSY_CONFIG = {
    "api_url": "https://apifeed.sellsy.com/0/",
    "consumer_token": os.getenv("SELLSY_CONSUMER_TOKEN", ""),
    "consumer_secret": os.getenv("SELLSY_CONSUMER_SECRET", ""),
    "user_token": os.getenv("SELLSY_USER_TOKEN", ""),
    "user_secret": os.getenv("SELLSY_USER_SECRET", ""),
}

# Sellsy API v2 (OAuth2 Client Credentials) — pour les fichiers/photos
SELLSY_V2_CONFIG = {
    "client_id": os.getenv("SELLSY_V2_CLIENT_ID", ""),
    "client_secret": os.getenv("SELLSY_V2_CLIENT_SECRET", ""),
}

# Étapes du pipeline Sellsy qu'on veut récupérer
ETAPES_CHANTIER = ["📆 Chantier à programmer", "👷🏼 Chantier à réaliser"]

# Équipe production
EQUIPE_PRODUCTION = ["William", "Geoffrey", "Romain"]

# ──────────────────────────────────────────────────────
# SELLSY CLIENT
# ──────────────────────────────────────────────────────

class SellsyClient:
    def __init__(self, config):
        self.api_url = config["api_url"]
        self.session = OAuth1Session(
            client_key=config["consumer_token"],
            client_secret=config["consumer_secret"],
            resource_owner_key=config["user_token"],
            resource_owner_secret=config["user_secret"],
        )

    def call(self, method, params=None, retries=3):
        for attempt in range(retries):
            do_in = json.dumps({"method": method, "params": params or {}})
            response = self.session.post(
                self.api_url,
                data={"request": 1, "io_mode": "json", "do_in": do_in},
            )
            if response.status_code == 429 or "LIMIT_REQUEST_REACHED" in response.text:
                time.sleep(2 + attempt * 3)
                continue
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text[:300]}")
            data = response.json()
            if data.get("status") == "error":
                err = data.get("error", {})
                if isinstance(err, dict) and "LIMIT" in str(err.get("message", "")):
                    time.sleep(2 + attempt * 3)
                    continue
                raise Exception(f"Sellsy error: {err}")
            # Petit délai entre les appels pour ne pas dépasser la limite
            time.sleep(0.5)
            return data.get("response", data)
        raise Exception(f"Rate limit dépassé après {retries} tentatives pour {method}")

    def call_paginated(self, method, params=None, max_pages=50):
        params = params or {}
        all_results = []
        for page in range(1, max_pages + 1):
            params["pagination"] = {"nbperpage": 100, "pagenum": page}
            resp = self.call(method, params)
            results = resp.get("result", {})
            if isinstance(results, dict):
                all_results.extend(results.values())
            elif isinstance(results, list):
                all_results.extend(results)
            total_pages = int(resp.get("infos", {}).get("nbpages", 1))
            if page >= total_pages:
                break
        return all_results


def get_sellsy_client():
    if not SELLSY_CONFIG["consumer_token"]:
        return None
    return SellsyClient(SELLSY_CONFIG)


class SellsyV2Client:
    """Client pour l'API Sellsy v2 — utilisé pour récupérer les fichiers/photos."""

    BASE_URL = "https://api.sellsy.com/v2"
    TOKEN_URL = "https://login.sellsy.com/oauth2/access-tokens"

    def __init__(self, config):
        self.client_id = config.get("client_id", "")
        self.client_secret = config.get("client_secret", "")
        self._access_token = None
        self._token_expiry = 0

    def _ensure_token(self):
        if self._access_token and time.time() < (self._token_expiry - 60):
            return
        resp = requests.post(self.TOKEN_URL, json={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        })
        if resp.status_code != 200:
            raise Exception(f"OAuth2 error: {resp.text[:300]}")
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600)

    def get(self, endpoint):
        self._ensure_token()
        resp = requests.get(
            f"{self.BASE_URL}{endpoint}",
            headers={"Authorization": f"Bearer {self._access_token}"},
        )
        if resp.status_code != 200:
            return None
        return resp.json()

    def get_opportunity_files(self, opp_id):
        """Récupère les fichiers (photos, PDF) liés à une opportunité."""
        data = self.get(f"/opportunities/{opp_id}/files")
        if not data:
            return []
        files = []
        for f in data.get("data", []):
            ext = (f.get("extension", "") or "").lower()
            files.append({
                "name": f.get("name", ""),
                "extension": ext,
                "size": f.get("size", 0),
                "public_link": f.get("public_link", ""),
                "is_image": ext in ("jpg", "jpeg", "png", "webp", "heic"),
                "created": f.get("created", ""),
            })
        return files


def get_sellsy_v2_client():
    if not SELLSY_V2_CONFIG["client_id"]:
        return None
    return SellsyV2Client(SELLSY_V2_CONFIG)


# ──────────────────────────────────────────────────────
# STOCKAGE LOCAL (JSON)
# ──────────────────────────────────────────────────────

CHANTIERS_FILE = DATA_DIR / "chantiers.json"


def load_chantiers():
    if CHANTIERS_FILE.exists():
        with open(CHANTIERS_FILE) as f:
            return json.load(f)
    return {}


def save_chantiers(chantiers):
    with open(CHANTIERS_FILE, "w") as f:
        json.dump(chantiers, f, indent=2, ensure_ascii=False)


def _strip_html(text):
    """Retire les balises HTML et nettoie le texte."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&nbsp;', ' ')
    return text.strip()


def _fetch_devis_lines(client, doc_id):
    """Récupère les lignes d'un devis Sellsy."""
    try:
        doc = client.call("Document.getOne", {"doctype": "estimate", "docid": doc_id})
        rows = doc.get("map", {}).get("rows", {})
        if isinstance(rows, dict):
            rows = [v for v in rows.values() if isinstance(v, dict)]
        elif not isinstance(rows, list):
            rows = []

        lines = []
        for row in rows:
            name = row.get("name", "")
            if not name:
                continue
            qt_raw = row.get("qt", "0")
            try:
                qt = float(qt_raw)
            except (ValueError, TypeError):
                qt = 0
            unit = row.get("unit", "")
            notes = _strip_html(row.get("notes", ""))
            unit_amount = row.get("unitAmount", "0")
            try:
                unit_amount = float(unit_amount)
            except (ValueError, TypeError):
                unit_amount = 0

            lines.append({
                "reference": name,
                "description": notes,
                "quantite": qt,
                "unite": unit,
                "prix_unitaire": unit_amount,
            })
        return lines
    except Exception as e:
        return [{"reference": "Erreur", "description": str(e), "quantite": 0, "unite": "", "prix_unitaire": 0}]


def _fetch_opp_address(client, opp_id):
    """Récupère l'adresse du chantier depuis l'opportunité."""
    try:
        resp = client.call("Opportunities.getOne", {"id": opp_id})
        # Adresse dans thirdDetails ou contacts
        third = resp.get("thirdDetails", {})
        if isinstance(third, dict):
            addr = third.get("address", third.get("addr", {}))
            if isinstance(addr, dict):
                parts = [
                    addr.get("part1", ""),
                    addr.get("part2", ""),
                    f"{addr.get('zip', '')} {addr.get('town', '')}".strip(),
                ]
                address = ", ".join(p for p in parts if p)
                if address:
                    return address
            # Try flat fields
            parts = [
                third.get("addressPart1", third.get("address", "")),
                f"{third.get('addressZip', '')} {third.get('addressTown', '')}".strip(),
            ]
            address = ", ".join(p for p in parts if p)
            if address:
                return address
        return ""
    except Exception:
        return ""


def sync_from_sellsy():
    """Synchronise les opportunités chantier depuis Sellsy avec lignes de devis et fichiers."""
    client = get_sellsy_client()
    if not client:
        return {"error": "Sellsy non configuré"}

    v2_client = get_sellsy_v2_client()

    chantiers = load_chantiers()
    nouvelles = 0

    try:
        opps = client.call_paginated("Opportunities.getList")
    except Exception as e:
        return {"error": str(e)}

    for opp in opps:
        step_label = opp.get("stepLabel", opp.get("step_label", ""))
        if step_label not in ETAPES_CHANTIER:
            continue

        opp_id = str(opp.get("id", opp.get("ident", "")))
        if not opp_id:
            continue

        # Si déjà dans notre base, on met à jour les infos Sellsy mais on garde les données saisies
        if opp_id in chantiers:
            chantiers[opp_id]["sellsy"] = _extract_opp_data(opp)

            # Re-fetch devis si manquant ou en erreur
            existing_lines = chantiers[opp_id]["sellsy"].get("devis_lines", [])
            has_error = any(l.get("reference") == "Erreur" for l in existing_lines)
            if not existing_lines or has_error:
                try:
                    detail = client.call("Opportunities.getOne", {"id": opp_id})
                    main_doc_id = detail.get("mainDocId")
                    if main_doc_id and str(main_doc_id) != "0":
                        chantiers[opp_id]["sellsy"]["devis_lines"] = _fetch_devis_lines(client, main_doc_id)
                        doc_info = client.call("Document.getOne", {"doctype": "estimate", "docid": main_doc_id})
                        chantiers[opp_id]["sellsy"]["devis_ref"] = doc_info.get("ident", "")
                except Exception:
                    pass

            # Mettre à jour les fichiers Sellsy
            if v2_client and not chantiers[opp_id].get("sellsy_files"):
                try:
                    chantiers[opp_id]["sellsy_files"] = v2_client.get_opportunity_files(opp_id)
                except Exception:
                    pass
            continue

        # Récupérer les détails enrichis
        opp_data = _extract_opp_data(opp)

        # Récupérer les lignes du devis principal
        detail = None
        try:
            detail = client.call("Opportunities.getOne", {"id": opp_id})
        except Exception:
            pass

        devis_lines = []
        devis_ref = ""
        if detail:
            main_doc_id = detail.get("mainDocId")
            if main_doc_id and str(main_doc_id) != "0":
                devis_lines = _fetch_devis_lines(client, main_doc_id)
                try:
                    doc_info = client.call("Document.getOne", {"doctype": "estimate", "docid": main_doc_id})
                    devis_ref = doc_info.get("ident", "")
                except Exception:
                    pass

        # Récupérer l'adresse
        address = _fetch_opp_address(client, opp_id)

        opp_data["adresse"] = address
        opp_data["devis_ref"] = devis_ref
        opp_data["devis_lines"] = devis_lines

        # Récupérer les fichiers/photos via API v2
        sellsy_files = []
        if v2_client:
            try:
                sellsy_files = v2_client.get_opportunity_files(opp_id)
            except Exception as e:
                logger.warning(f"Erreur fichiers opp {opp_id}: {e}")

        # Nouveau chantier
        chantiers[opp_id] = {
            "id": opp_id,
            "sellsy": opp_data,
            "sellsy_files": sellsy_files,
            "etape": "a_preparer",
            "preparation": {},
            "commande": {},
            "programmation": {},
            "photos": [],
            "historique": [{
                "action": "Importé depuis Sellsy",
                "par": "système",
                "date": datetime.now().isoformat(),
            }],
            "created_at": datetime.now().isoformat(),
        }
        nouvelles += 1

    save_chantiers(chantiers)
    return {"nouvelles": nouvelles, "total": len(chantiers)}


def _extract_opp_data(opp):
    """Extrait les données utiles d'une opportunité Sellsy."""
    amount = opp.get("potential", opp.get("amount", opp.get("estimateAmount", 0)))
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        amount = 0

    client_name = opp.get("thirdName", opp.get("linkedName", ""))
    contact_name = opp.get("contactName", opp.get("contactFullName", ""))

    return {
        "nom": opp.get("name", opp.get("ident", "")),
        "client": client_name,
        "contact": contact_name,
        "montant": amount,
        "step": opp.get("stepLabel", opp.get("step_label", "")),
        "commercial": opp.get("ownerFullName", ""),
        "created": opp.get("created", ""),
        "description": opp.get("description", ""),
    }


# ──────────────────────────────────────────────────────
# CALCUL ÉTAPE (non séquentiel)
# ──────────────────────────────────────────────────────

def _compute_etape(ch):
    """Calcule l'étape du chantier selon les validations faites (ordre libre)."""
    has_prepa = bool(ch.get("preparation", {}).get("valide_par"))
    has_commande = bool(ch.get("commande", {}).get("valide_par"))
    has_prog = bool(ch.get("programmation", {}).get("valide_par"))

    if has_prepa and has_commande and has_prog:
        return "pret"

    # Compter combien d'étapes sont validées
    done = sum([has_prepa, has_commande, has_prog])
    if done == 0:
        return "en_cours"  # rien validé encore
    if done >= 1 and not (has_prepa and has_commande and has_prog):
        return "en_cours"  # partiellement validé

    return "pret"


# ──────────────────────────────────────────────────────
# AUTH (cookies simples)
# ──────────────────────────────────────────────────────

def make_token(username):
    secret = os.getenv("SESSION_SECRET", "cdba-chantiers-2026")
    return hashlib.sha256(f"{username}:{secret}".encode()).hexdigest()[:32]


def get_current_user(request: Request):
    token = request.cookies.get("session")
    if not token:
        return None
    for username, info in USERS.items():
        if make_token(username) == token:
            return {"username": username, **info}
    return None


# ──────────────────────────────────────────────────────
# ROUTES
# ──────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    return RedirectResponse("/board", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    username = username.lower().strip()
    user = USERS.get(username)
    if not user or user["password"] != password:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Identifiant ou mot de passe incorrect"
        })
    response = RedirectResponse("/board", status_code=302)
    response.set_cookie("session", make_token(username), max_age=86400 * 30, httponly=True)
    return response


@app.get("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=302)
    response.delete_cookie("session")
    return response


@app.get("/board", response_class=HTMLResponse)
async def board(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    chantiers = load_chantiers()

    colonnes = {
        "en_cours": {"label": "En cours", "color": "#f97316", "icon": "🟠", "chantiers": []},
        "pret": {"label": "Prêt", "color": "#22c55e", "icon": "🟢", "chantiers": []},
    }

    for ch in sorted(chantiers.values(), key=lambda x: x.get("created_at", ""), reverse=True):
        # Recalculer l'étape
        etape = _compute_etape(ch)
        ch["etape"] = etape
        if etape in colonnes:
            colonnes[etape]["chantiers"].append(ch)

    return templates.TemplateResponse("board.html", {
        "request": request,
        "user": user,
        "colonnes": colonnes,
        "equipe": EQUIPE_PRODUCTION,
    })


@app.get("/chantier/{chantier_id}", response_class=HTMLResponse)
async def chantier_detail(request: Request, chantier_id: str):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    chantiers = load_chantiers()
    ch = chantiers.get(chantier_id)
    if not ch:
        raise HTTPException(404, "Chantier non trouvé")

    return templates.TemplateResponse("chantier.html", {
        "request": request,
        "user": user,
        "ch": ch,
        "equipe": EQUIPE_PRODUCTION,
    })


@app.post("/chantier/{chantier_id}/preparation")
async def save_preparation(
    request: Request,
    chantier_id: str,
    nb_personnes: int = Form(...),
    nb_jours: int = Form(...),
    equipe: list = Form(default=[]),
    materiaux: str = Form(""),
    notes: str = Form(""),
):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    chantiers = load_chantiers()
    ch = chantiers.get(chantier_id)
    if not ch:
        raise HTTPException(404)

    ch["preparation"] = {
        "nb_personnes": nb_personnes,
        "nb_jours": nb_jours,
        "equipe": equipe,
        "materiaux": materiaux,
        "notes": notes,
        "valide_par": user["name"],
        "valide_le": datetime.now().isoformat(),
    }
    ch["etape"] = _compute_etape(ch)
    ch["historique"].append({
        "action": f"Préparation validée ({nb_jours}j, {nb_personnes} pers.)",
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    save_chantiers(chantiers)
    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


@app.post("/chantier/{chantier_id}/commande")
async def save_commande(
    request: Request,
    chantier_id: str,
    fournisseur: str = Form(""),
    reference_commande: str = Form(""),
    notes: str = Form(""),
):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    chantiers = load_chantiers()
    ch = chantiers.get(chantier_id)
    if not ch:
        raise HTTPException(404)

    ch["commande"] = {
        "fournisseur": fournisseur,
        "reference_commande": reference_commande,
        "notes": notes,
        "valide_par": user["name"],
        "valide_le": datetime.now().isoformat(),
    }
    ch["etape"] = _compute_etape(ch)
    ch["historique"].append({
        "action": "Commande matériaux validée",
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    save_chantiers(chantiers)
    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


@app.post("/chantier/{chantier_id}/programmation")
async def save_programmation(
    request: Request,
    chantier_id: str,
    semaine: str = Form(""),
    date_debut: str = Form(""),
    notes: str = Form(""),
):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    chantiers = load_chantiers()
    ch = chantiers.get(chantier_id)
    if not ch:
        raise HTTPException(404)

    ch["programmation"] = {
        "semaine": semaine,
        "date_debut": date_debut,
        "notes": notes,
        "valide_par": user["name"],
        "valide_le": datetime.now().isoformat(),
    }
    ch["etape"] = _compute_etape(ch)
    ch["historique"].append({
        "action": f"Programmé semaine {semaine}" if semaine else "Programmé",
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    save_chantiers(chantiers)
    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


@app.post("/chantier/{chantier_id}/photos")
async def upload_photos(
    request: Request,
    chantier_id: str,
    photos: list[UploadFile] = File(...),
):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    chantiers = load_chantiers()
    ch = chantiers.get(chantier_id)
    if not ch:
        raise HTTPException(404)

    if "photos" not in ch:
        ch["photos"] = []

    chantier_uploads = UPLOADS_DIR / chantier_id
    chantier_uploads.mkdir(exist_ok=True)

    for photo in photos:
        if not photo.filename:
            continue
        ext = Path(photo.filename).suffix.lower()
        if ext not in ('.jpg', '.jpeg', '.png', '.webp', '.heic'):
            continue
        file_id = uuid.uuid4().hex[:8]
        filename = f"{file_id}{ext}"
        filepath = chantier_uploads / filename
        content = await photo.read()
        with open(filepath, "wb") as f:
            f.write(content)

        ch["photos"].append({
            "filename": filename,
            "original_name": photo.filename,
            "uploaded_by": user["name"],
            "uploaded_at": datetime.now().isoformat(),
        })

    ch["historique"].append({
        "action": f"{len(photos)} photo(s) ajoutée(s)",
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    save_chantiers(chantiers)
    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


@app.post("/sync")
async def sync(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    result = sync_from_sellsy()
    return RedirectResponse("/board", status_code=302)


@app.get("/api/sync")
async def api_sync(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    result = sync_from_sellsy()
    return JSONResponse(result)
