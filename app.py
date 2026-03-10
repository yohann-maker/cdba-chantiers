#!/usr/bin/env python3
"""
CDBA — App Préparation Chantiers
Workflow : Sellsy → William (prépa) → Julien (commande) → Gina/Charlotte (planning) → Prêt
"""

import json
import os
import hashlib
import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request, Form, HTTPException, Response
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from requests_oauthlib import OAuth1Session
import requests

# ──────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────

app = FastAPI(title="CDBA Chantiers")

BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

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

# Étapes du pipeline Sellsy qu'on veut récupérer
ETAPES_CHANTIER = ["Chantier à programmer", "Chantier programmé"]

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

    def call(self, method, params=None):
        do_in = json.dumps({"method": method, "params": params or {}})
        response = self.session.post(
            self.api_url,
            data={"request": 1, "io_mode": "json", "do_in": do_in},
        )
        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}: {response.text[:300]}")
        data = response.json()
        if data.get("status") == "error":
            raise Exception(f"Sellsy error: {data.get('error', {})}")
        return data.get("response", data)

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


def sync_from_sellsy():
    """Synchronise les opportunités 'Chantier à programmer' depuis Sellsy."""
    client = get_sellsy_client()
    if not client:
        return {"error": "Sellsy non configuré"}

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
            continue

        # Nouveau chantier
        chantiers[opp_id] = {
            "id": opp_id,
            "sellsy": _extract_opp_data(opp),
            "etape": "a_preparer",
            "preparation": {},
            "commande": {},
            "programmation": {},
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
    # Montant
    amount = opp.get("amount", opp.get("estimateAmount", 0))
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        amount = 0

    # Client
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

    # Organiser par étape
    colonnes = {
        "a_preparer": {"label": "À préparer", "color": "#ef4444", "icon": "🔴", "chantiers": []},
        "a_commander": {"label": "À commander", "color": "#f97316", "icon": "🟠", "chantiers": []},
        "a_programmer": {"label": "À programmer", "color": "#eab308", "icon": "🟡", "chantiers": []},
        "pret": {"label": "Prêt", "color": "#22c55e", "icon": "🟢", "chantiers": []},
    }

    for ch in sorted(chantiers.values(), key=lambda x: x.get("created_at", ""), reverse=True):
        etape = ch.get("etape", "a_preparer")
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
    ch["etape"] = "a_commander"
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
    ch["etape"] = "a_programmer"
    ch["historique"].append({
        "action": f"Commande matériaux validée",
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
    ch["etape"] = "pret"
    ch["historique"].append({
        "action": f"Programmé semaine {semaine}" if semaine else "Programmé",
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
