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
import threading
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI, Request, Form, HTTPException, Response, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from requests_oauthlib import OAuth1Session
import requests

logger = logging.getLogger("cdba")

# Slack webhook pour notifier les ouvriers (canal #bdc-et-photos-chantiers)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_CHANTIERS", "")

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
    "olivier": {"name": "Olivier", "role": "admin", "password": os.getenv("PASS_OLIVIER", "cdba2026")},
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
EQUIPE_PRODUCTION = ["William", "Geoffrey", "Romain", "Julien", "Didier", "Sous-traitant"]

# Google Calendar OAuth2 (refresh token)
GOOGLE_CAL_CONFIG = {
    "client_id": os.getenv("GOOGLE_CLIENT_ID", ""),
    "client_secret": os.getenv("GOOGLE_CLIENT_SECRET", ""),
    "refresh_token": os.getenv("GOOGLE_REFRESH_TOKEN", ""),
}

# Calendriers des ouvriers
CALENDRIERS_OUVRIERS = {
    "William": "william@groupe-cdba.fr",
    "Geoffrey": "geoffrey@groupe-cdba.fr",
    "Romain": "romain@groupe-cdba.fr",
    "Didier": "didier@groupe-cdba.fr",
    "Julien": "julien@groupe-cdba.fr",
    "Sous-traitant": "c_d274260d63816cecc49f51912e95ee01fc365affee0d574df96612495d178d82@group.calendar.google.com",
}


# ──────────────────────────────────────────────────────
# GOOGLE CALENDAR CLIENT
# ──────────────────────────────────────────────────────

class GoogleCalendarClient:
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    API_BASE = "https://www.googleapis.com/calendar/v3"

    def __init__(self, config):
        self.client_id = config["client_id"]
        self.client_secret = config["client_secret"]
        self.refresh_token = config["refresh_token"]
        self._access_token = None
        self._token_expiry = 0

    def _ensure_token(self):
        if self._access_token and time.time() < (self._token_expiry - 60):
            return
        resp = requests.post(self.TOKEN_URL, data={
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        })
        if resp.status_code != 200:
            raise Exception(f"Google OAuth error: {resp.text[:300]}")
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600)

    def _headers(self):
        self._ensure_token()
        return {"Authorization": f"Bearer {self._access_token}"}

    def search_events(self, calendar_id, query, time_min=None, time_max=None):
        """Cherche des events dans un calendrier par mot-clé."""
        if not time_min:
            time_min = datetime.utcnow().strftime("%Y-%m-%dT00:00:00Z")
        if not time_max:
            time_max = (datetime.utcnow() + timedelta(weeks=8)).strftime("%Y-%m-%dT23:59:59Z")
        params = {
            "q": query,
            "timeMin": time_min,
            "timeMax": time_max,
            "singleEvents": "true",
            "maxResults": 50,
        }
        resp = requests.get(
            f"{self.API_BASE}/calendars/{calendar_id}/events",
            headers=self._headers(),
            params=params,
        )
        if resp.status_code != 200:
            logger.warning(f"Google Calendar search error ({calendar_id}): {resp.status_code}")
            return []
        return resp.json().get("items", [])

    def create_event(self, calendar_id, summary, start_date, end_date, description="", location=""):
        """Crée un event journée entière."""
        event = {
            "summary": summary,
            "start": {"date": start_date},
            "end": {"date": end_date},
        }
        if description:
            event["description"] = description
        if location:
            event["location"] = location
        resp = requests.post(
            f"{self.API_BASE}/calendars/{calendar_id}/events",
            headers={**self._headers(), "Content-Type": "application/json"},
            json=event,
        )
        if resp.status_code not in (200, 201):
            raise Exception(f"Google Calendar create error: {resp.status_code} {resp.text[:300]}")
        return resp.json()


def get_gcal_client():
    if not GOOGLE_CAL_CONFIG["refresh_token"]:
        return None
    return GoogleCalendarClient(GOOGLE_CAL_CONFIG)


def _extract_client_nom(client_name):
    """Extrait le nom de famille pour la détection de doublons."""
    if not client_name:
        return ""
    parts = client_name.strip().split()
    # Prend le dernier mot comme nom de famille, ou le premier si c'est en MAJUSCULES
    for part in parts:
        if part == part.upper() and len(part) > 2:
            return part
    return parts[-1] if parts else ""


def _build_event_title(ch):
    """Construit le titre de l'event Calendar : 🤖 NOM Client / Type travaux."""
    client = ch.get("sellsy", {}).get("client", "")
    nom_opp = ch.get("sellsy", {}).get("nom", "")
    return f"🤖 {client} / {nom_opp}"


def create_calendar_events(ch):
    """Crée les events Google Calendar pour un chantier programmé.

    Retourne le nombre d'events créés et les éventuels doublons détectés.
    """
    gcal = get_gcal_client()
    if not gcal:
        return 0, ["Google Calendar non configuré"]

    preparation = ch.get("preparation", {})
    programmation = ch.get("programmation", {})
    sellsy = ch.get("sellsy", {})

    equipe = preparation.get("equipe", [])
    nb_jours = preparation.get("nb_jours", 1)
    date_debut = programmation.get("date_debut", "")

    if not equipe or not date_debut:
        return 0, ["Équipe ou date de début manquante"]

    # Calculer date de fin (all-day events : end = jour APRÈS le dernier jour)
    try:
        start = datetime.strptime(date_debut, "%Y-%m-%d")
        end = start + timedelta(days=nb_jours)
        end_date = end.strftime("%Y-%m-%d")
    except ValueError:
        return 0, [f"Format de date invalide : {date_debut}"]

    # Préparer le contenu
    title = _build_event_title(ch)
    client_name = sellsy.get("client", "")
    montant = sellsy.get("montant", 0)
    contact = sellsy.get("contact", "")
    mobile = sellsy.get("mobile", "")
    adresse = sellsy.get("adresse", "")
    commercial = sellsy.get("commercial", "")
    nb_personnes = preparation.get("nb_personnes", "")
    equipe_noms = ", ".join(equipe)

    # Description complète
    desc_lines = []
    if mobile:
        desc_lines.append(f"Mobile : {mobile}")
    if contact:
        desc_lines.append(f"Contact : {contact}")
    desc_lines.append(f"Montant : {montant:,.0f} € HT".replace(",", " "))
    if commercial:
        desc_lines.append(f"Commercial : {commercial}")
    desc_lines.append(f"Équipe : {equipe_noms} ({nb_personnes} pers.)")
    desc_lines.append(f"Durée : {nb_jours} jour(s)")
    description = "\n".join(desc_lines)

    # Détection doublons + création
    nom_recherche = _extract_client_nom(client_name)
    created = 0
    messages = []

    for membre in equipe:
        cal_id = CALENDRIERS_OUVRIERS.get(membre)
        if not cal_id:
            messages.append(f"{membre} : pas de calendrier configuré")
            continue

        # Vérifier doublon
        if nom_recherche:
            existing = gcal.search_events(cal_id, nom_recherche)
            if existing:
                event_titles = [e.get("summary", "") for e in existing]
                messages.append(f"{membre} : doublon détecté ({nom_recherche}) → {event_titles[0]}")
                continue

        # Créer l'event
        try:
            gcal.create_event(
                calendar_id=cal_id,
                summary=title,
                start_date=date_debut,
                end_date=end_date,
                description=description,
                location=adresse,
            )
            created += 1
        except Exception as e:
            messages.append(f"{membre} : erreur création → {str(e)[:100]}")

    return created, messages


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
    """Récupère l'adresse du chantier depuis l'opportunité.
    Retourne (adresse_complete, ville, cp).
    """
    try:
        resp = client.call("Opportunities.getOne", {"id": opp_id})
        third = resp.get("thirdDetails", {})
        if isinstance(third, dict):
            addr = third.get("address", third.get("addr", {}))
            if isinstance(addr, dict):
                zip_code = addr.get("zip", "")
                town = addr.get("town", "")
                parts = [
                    addr.get("part1", ""),
                    addr.get("part2", ""),
                    f"{zip_code} {town}".strip(),
                ]
                address = ", ".join(p for p in parts if p)
                if address:
                    return address, town, zip_code
            # Try flat fields
            zip_code = third.get("addressZip", "")
            town = third.get("addressTown", "")
            parts = [
                third.get("addressPart1", third.get("address", "")),
                f"{zip_code} {town}".strip(),
            ]
            address = ", ".join(p for p in parts if p)
            if address:
                return address, town, zip_code
        return "", "", ""
    except Exception:
        return "", "", ""


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
            s = chantiers[opp_id]["sellsy"]

            # Enrichir adresse/ville/CP et prénom si manquants
            if not s.get("ville") or not s.get("contact_prenom"):
                try:
                    detail_existing = client.call("Opportunities.getOne", {"id": opp_id})
                    if not s.get("ville"):
                        addr, ville, cp = _fetch_opp_address(client, opp_id)
                        s["adresse"] = addr
                        s["ville"] = ville
                        s["cp"] = cp
                    if not s.get("contact_prenom"):
                        cid = detail_existing.get("contactId", detail_existing.get("contact_id", ""))
                        if cid and str(cid) != "0":
                            contact_data = client.call("Peoples.getOne", {"id": cid})
                            s["contact_prenom"] = (
                                contact_data.get("forename", "")
                                or contact_data.get("firstname", "")
                                or contact_data.get("first_name", "")
                                or ""
                            )
                except Exception:
                    pass

            # Re-fetch devis si manquant ou en erreur
            existing_lines = s.get("devis_lines", [])
            has_error = any(l.get("reference") == "Erreur" for l in existing_lines)
            if not existing_lines or has_error:
                try:
                    detail = client.call("Opportunities.getOne", {"id": opp_id})
                    main_doc_id = detail.get("mainDocId")
                    if main_doc_id and str(main_doc_id) != "0":
                        s["devis_lines"] = _fetch_devis_lines(client, main_doc_id)
                        doc_info = client.call("Document.getOne", {"doctype": "estimate", "docid": main_doc_id})
                        s["devis_ref"] = doc_info.get("ident", "")
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
        address, ville, cp = _fetch_opp_address(client, opp_id)

        # Récupérer le mobile et prénom du contact si pas déjà dans l'opp
        if detail:
            contact_id = detail.get("contactId", detail.get("contact_id", ""))
            if contact_id and str(contact_id) != "0":
                try:
                    contact_data = client.call("Peoples.getOne", {"id": contact_id})
                    if not opp_data.get("mobile"):
                        opp_data["mobile"] = (
                            contact_data.get("mobile", "")
                            or contact_data.get("phoneMobile", "")
                            or contact_data.get("phone", "")
                            or ""
                        )
                    if not opp_data.get("contact_prenom"):
                        opp_data["contact_prenom"] = (
                            contact_data.get("forename", "")
                            or contact_data.get("firstname", "")
                            or contact_data.get("first_name", "")
                            or ""
                        )
                except Exception:
                    pass

        opp_data["adresse"] = address
        opp_data["ville"] = ville
        opp_data["cp"] = cp
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
    contact_prenom = opp.get("contactForename", opp.get("contactFirstName", ""))

    # Téléphone mobile — on cherche dans plusieurs champs
    mobile = (
        opp.get("contactMobile", "")
        or opp.get("contactPhone", "")
        or opp.get("thirdMobile", "")
        or opp.get("thirdPhone", "")
        or opp.get("phoneMobile", "")
        or opp.get("mobile", "")
        or ""
    )

    return {
        "nom": opp.get("name", opp.get("ident", "")),
        "client": client_name,
        "contact": contact_name,
        "contact_prenom": contact_prenom,
        "mobile": mobile,
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


LAST_SYNC_FILE = DATA_DIR / "last_sync.txt"
SYNC_INTERVAL = 900  # 15 minutes
_sync_lock = threading.Lock()
_sync_running = False


def _should_auto_sync():
    """Vérifie si le dernier sync date de plus de 15 minutes."""
    if not LAST_SYNC_FILE.exists():
        return True
    try:
        ts = float(LAST_SYNC_FILE.read_text().strip())
        return (time.time() - ts) > SYNC_INTERVAL
    except (ValueError, OSError):
        return True


def _record_sync():
    LAST_SYNC_FILE.write_text(str(time.time()))


def _background_sync():
    """Lance le sync Sellsy en arrière-plan."""
    global _sync_running
    try:
        _sync_running = True
        sync_from_sellsy()
        _record_sync()
    except Exception as e:
        logger.warning(f"Erreur sync arrière-plan : {e}")
    finally:
        _sync_running = False


@app.get("/board", response_class=HTMLResponse)
async def board(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    # Auto-sync en arrière-plan (ne bloque PAS la page)
    if _should_auto_sync() and not _sync_running:
        with _sync_lock:
            if not _sync_running:
                thread = threading.Thread(target=_background_sync, daemon=True)
                thread.start()

    chantiers = load_chantiers()
    syncing = _sync_running

    colonnes = {
        "en_cours": {"label": "En cours", "color": "#f97316", "icon": "🟠", "chantiers": []},
        "pret": {"label": "Prêt", "color": "#22c55e", "icon": "🟢", "chantiers": []},
        "termine": {"label": "Terminé", "color": "#64748b", "icon": "✅", "chantiers": []},
    }

    for ch in sorted(chantiers.values(), key=lambda x: x.get("created_at", ""), reverse=True):
        # Enrichir ville/CP depuis l'adresse si pas encore présent
        sellsy = ch.get("sellsy", {})
        if not sellsy.get("ville") and sellsy.get("adresse"):
            m = re.search(r"(\d{5})\s+(.+?)(?:,|$)", sellsy["adresse"])
            if m:
                sellsy["cp"] = m.group(1)
                sellsy["ville"] = m.group(2).strip()
        # Chantier marqué terminé = colonne terminé
        if ch.get("termine", {}).get("valide_par"):
            ch["etape"] = "termine"
            colonnes["termine"]["chantiers"].append(ch)
            continue
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
        "syncing": syncing,
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
async def save_commande(request: Request, chantier_id: str):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    form = await request.form()
    fournisseur = form.get("fournisseur", "")
    reference_commande = form.get("reference_commande", "")
    notes = form.get("notes", "")

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
        "factures": ch.get("commande", {}).get("factures", []),
    }
    ch["etape"] = _compute_etape(ch)
    ch["historique"].append({
        "action": "Commande matériaux validée",
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    save_chantiers(chantiers)
    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


@app.post("/chantier/{chantier_id}/facture")
async def upload_facture(request: Request, chantier_id: str):
    """Upload une facture matériaux (photo ou PDF)."""
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    form = await request.form()
    fichier = form.get("facture")
    if not fichier or not fichier.filename:
        return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)

    chantiers = load_chantiers()
    ch = chantiers.get(chantier_id)
    if not ch:
        raise HTTPException(404)

    ext = Path(fichier.filename).suffix.lower()
    if ext not in ('.jpg', '.jpeg', '.png', '.webp', '.heic', '.pdf'):
        raise HTTPException(400, "Format non supporté (JPG, PNG, PDF)")

    chantier_uploads = UPLOADS_DIR / chantier_id
    chantier_uploads.mkdir(exist_ok=True)

    file_id = uuid.uuid4().hex[:8]
    filename = f"facture_{file_id}{ext}"
    filepath = chantier_uploads / filename
    content = await fichier.read()
    with open(filepath, "wb") as f:
        f.write(content)

    if "commande" not in ch:
        ch["commande"] = {}
    if "factures" not in ch["commande"]:
        ch["commande"]["factures"] = []

    ch["commande"]["factures"].append({
        "filename": filename,
        "original_name": fichier.filename,
        "uploaded_by": user["name"],
        "uploaded_at": datetime.now().isoformat(),
    })

    ch["historique"].append({
        "action": f"Facture ajoutée : {fichier.filename}",
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    save_chantiers(chantiers)
    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


def _get_busy_dates(gcal, calendar_id, weeks=8):
    """Récupère les dates occupées d'un calendrier sur les N prochaines semaines."""
    now = datetime.utcnow()
    time_min = now.strftime("%Y-%m-%dT00:00:00Z")
    time_max = (now + timedelta(weeks=weeks)).strftime("%Y-%m-%dT23:59:59Z")

    busy = set()
    try:
        params = {
            "timeMin": time_min,
            "timeMax": time_max,
            "singleEvents": "true",
            "maxResults": 250,
        }
        resp = requests.get(
            f"{gcal.API_BASE}/calendars/{calendar_id}/events",
            headers=gcal._headers(),
            params=params,
        )
        if resp.status_code != 200:
            return busy
        for ev in resp.json().get("items", []):
            start = ev.get("start", {})
            end = ev.get("end", {})
            # Event journée entière
            if "date" in start:
                s = datetime.strptime(start["date"], "%Y-%m-%d")
                e = datetime.strptime(end["date"], "%Y-%m-%d")
                d = s
                while d < e:
                    busy.add(d.strftime("%Y-%m-%d"))
                    d += timedelta(days=1)
            # Event avec horaire (on bloque la journée)
            elif "dateTime" in start:
                dt = start["dateTime"][:10]
                busy.add(dt)
    except Exception:
        pass
    return busy


# Jours fériés France 2026
JOURS_FERIES_2026 = {
    "2026-01-01", "2026-04-06", "2026-04-07", "2026-05-01", "2026-05-08",
    "2026-05-14", "2026-05-25", "2026-07-14", "2026-08-15", "2026-11-01",
    "2026-11-11", "2026-12-25",
}


def _find_earliest_slot(equipe, nb_jours):
    """Trouve le premier créneau de nb_jours consécutifs (lun-ven) où toute l'équipe est libre.

    Commence à chercher à partir de demain, sur 8 semaines.
    Retourne la date de début (str YYYY-MM-DD) ou None.
    """
    gcal = get_gcal_client()
    if not gcal:
        return None

    # Collecter les dates occupées pour chaque membre de l'équipe
    all_busy = set()
    for membre in equipe:
        cal_id = CALENDRIERS_OUVRIERS.get(membre)
        if not cal_id:
            continue
        busy = _get_busy_dates(gcal, cal_id)
        all_busy |= busy  # Union : une date est bloquée si UN SEUL membre est occupé

    # Chercher le premier créneau libre
    today = datetime.now()
    # Commencer au prochain jour ouvré (min demain)
    candidate = today + timedelta(days=1)

    for _ in range(56):  # 8 semaines max
        # Vérifier que nb_jours consécutifs sont libres
        slot_ok = True
        check_date = candidate
        days_found = 0

        while days_found < nb_jours:
            date_str = check_date.strftime("%Y-%m-%d")

            # Sauter week-end et fériés
            if check_date.weekday() >= 5 or date_str in JOURS_FERIES_2026:
                check_date += timedelta(days=1)
                continue

            if date_str in all_busy:
                slot_ok = False
                break

            days_found += 1
            if days_found < nb_jours:
                check_date += timedelta(days=1)

        if slot_ok:
            return candidate.strftime("%Y-%m-%d")

        # Passer au jour ouvré suivant
        candidate += timedelta(days=1)
        while candidate.weekday() >= 5 or candidate.strftime("%Y-%m-%d") in JOURS_FERIES_2026:
            candidate += timedelta(days=1)

    return None


@app.post("/chantier/{chantier_id}/programmation")
async def save_programmation(
    request: Request,
    chantier_id: str,
    mode: str = Form("auto"),
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

    equipe = ch.get("preparation", {}).get("equipe", [])
    nb_jours = ch.get("preparation", {}).get("nb_jours", 1)

    # Mode auto : trouver le premier créneau libre
    if mode == "auto" and equipe:
        found_date = _find_earliest_slot(equipe, nb_jours)
        if found_date:
            date_debut = found_date
        else:
            ch["historique"].append({
                "action": "Programmation auto : aucun créneau trouvé sur 8 semaines",
                "par": "système",
                "date": datetime.now().isoformat(),
            })
            save_chantiers(chantiers)
            return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)

    if not date_debut:
        # Fallback : pas de date fournie et pas d'auto
        save_chantiers(chantiers)
        return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)

    try:
        start_dt = datetime.strptime(date_debut, "%Y-%m-%d")
        semaine = str(start_dt.isocalendar()[1])
    except ValueError:
        semaine = ""

    ch["programmation"] = {
        "semaine": semaine,
        "date_debut": date_debut,
        "mode": mode,
        "notes": notes,
        "valide_par": user["name"],
        "valide_le": datetime.now().isoformat(),
    }
    ch["etape"] = _compute_etape(ch)

    action_label = f"Programmé auto S{semaine} ({date_debut})" if mode == "auto" else f"Programmé manuellement S{semaine}"
    ch["historique"].append({
        "action": action_label,
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    # Création Google Calendar + récap (tous les utilisateurs autorisés, pas que Gina)
    if equipe and date_debut:
        try:
            created, messages = create_calendar_events(ch)
            cal_action = f"Agenda : {created} event(s) créé(s)"
            if messages:
                cal_action += f" — {'; '.join(messages)}"
            ch["historique"].append({
                "action": cal_action,
                "par": "système",
                "date": datetime.now().isoformat(),
            })
            try:
                _update_weekly_recap(ch)
            except Exception as e:
                logger.warning(f"Erreur MAJ récap hebdo : {e}")
        except Exception as e:
            ch["historique"].append({
                "action": f"Erreur création agenda : {str(e)[:100]}",
                "par": "système",
                "date": datetime.now().isoformat(),
            })

    save_chantiers(chantiers)
    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


def _update_weekly_recap(ch):
    """Met à jour l'event récap 📊 du samedi de la semaine de FIN du chantier.

    Règle : chantiers multi-semaines comptés dans la semaine de FIN.
    Format titre : 📊 Récap S{num} — {total} € HT
    Format description : une ligne par chantier (CLIENT : montant € HT)
    """
    gcal = get_gcal_client()
    if not gcal:
        return

    programmation = ch.get("programmation", {})
    preparation = ch.get("preparation", {})
    date_debut = programmation.get("date_debut", "")
    montant = ch.get("sellsy", {}).get("montant", 0)
    nb_jours = preparation.get("nb_jours", 1)
    if not date_debut or not montant:
        return

    try:
        start = datetime.strptime(date_debut, "%Y-%m-%d")
    except ValueError:
        return

    # Date de fin du chantier (dernier jour ouvré)
    end = start + timedelta(days=max(nb_jours - 1, 0))

    # Trouver le samedi de la semaine de FIN
    days_to_saturday = (5 - end.weekday()) % 7
    if days_to_saturday == 0 and end.weekday() != 5:
        days_to_saturday = 7  # si dimanche, samedi suivant
    saturday = end + timedelta(days=days_to_saturday)
    sat_str = saturday.strftime("%Y-%m-%d")
    sun_str = (saturday + timedelta(days=1)).strftime("%Y-%m-%d")
    week_num = saturday.isocalendar()[1]

    yohann_cal = "yohann@groupe-cdba.fr"
    client_name = ch.get("sellsy", {}).get("client", "")

    # Chercher un event 📊 existant ce samedi
    existing = gcal.search_events(
        yohann_cal, "📊",
        time_min=f"{sat_str}T00:00:00Z",
        time_max=f"{sun_str}T00:00:00Z",
    )

    recap_event = None
    for ev in existing:
        if "📊" in ev.get("summary", ""):
            recap_event = ev
            break

    new_line = f"{client_name} : {montant:.0f} € HT"

    if recap_event:
        # Mettre à jour l'event existant
        desc = recap_event.get("description", "")

        # Éviter les doublons (si même client déjà listé)
        if client_name and client_name in desc:
            return

        # Ajouter la nouvelle ligne
        lines = [l.strip() for l in desc.strip().split("\n") if l.strip()]
        lines.append(new_line)

        # Recalculer le total depuis toutes les lignes
        total = 0
        for line in lines:
            match = re.search(r"([\d\s]+)\s*€", line.replace("\u202f", "").replace(" ", ""))
            if match:
                try:
                    total += int(match.group(1).replace(" ", ""))
                except ValueError:
                    pass

        updated_title = f"📊 Récap S{week_num} — {total:,.0f} € HT".replace(",", " ")
        updated_desc = "\n".join(lines)

        event_id = recap_event["id"]
        try:
            requests.patch(
                f"{gcal.API_BASE}/calendars/{yohann_cal}/events/{event_id}",
                headers={**gcal._headers(), "Content-Type": "application/json"},
                json={"summary": updated_title, "description": updated_desc},
            )
        except Exception:
            pass

    else:
        # Créer un nouvel event récap 📊
        title = f"📊 Récap S{week_num} — {montant:,.0f} € HT".replace(",", " ")
        try:
            gcal.create_event(
                calendar_id=yohann_cal,
                summary=title,
                start_date=sat_str,
                end_date=sun_str,
                description=new_line,
            )
        except Exception:
            pass


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


@app.post("/chantier/{chantier_id}/termine")
async def save_termine(
    request: Request,
    chantier_id: str,
    jours_reels: float = Form(...),
    notes: str = Form(""),
):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    chantiers = load_chantiers()
    ch = chantiers.get(chantier_id)
    if not ch:
        raise HTTPException(404)

    nb_jours_prevu = ch.get("preparation", {}).get("nb_jours", 0)

    ch["termine"] = {
        "jours_reels": jours_reels,
        "jours_prevus": nb_jours_prevu,
        "notes": notes,
        "valide_par": user["name"],
        "valide_le": datetime.now().isoformat(),
    }
    ch["etape"] = "termine"
    ch["historique"].append({
        "action": f"Chantier terminé — {jours_reels}j réels (prévu {nb_jours_prevu}j)",
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    save_chantiers(chantiers)
    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


@app.post("/chantier/{chantier_id}/reset/{step}")
async def reset_step(request: Request, chantier_id: str, step: str):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    if step not in ("preparation", "commande", "programmation", "termine"):
        raise HTTPException(400, "Étape invalide")

    chantiers = load_chantiers()
    ch = chantiers.get(chantier_id)
    if not ch:
        raise HTTPException(404)

    old_data = ch.get(step, {})
    old_by = old_data.get("valide_par", "?")

    ch[step] = {}
    ch["etape"] = _compute_etape(ch)
    ch["historique"].append({
        "action": f"{step.capitalize()} réinitialisée (était validée par {old_by})",
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    save_chantiers(chantiers)
    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


def _send_slack_note(ch, note_text, author):
    """Envoie une notification Slack quand une note est ajoutée sur un chantier."""
    if not SLACK_WEBHOOK_URL:
        return
    try:
        sellsy = ch.get("sellsy", {})
        client = sellsy.get("client", "?")
        prenom = sellsy.get("contact_prenom", "")
        nom_opp = sellsy.get("nom", "")
        ville = sellsy.get("ville", "")
        cp = sellsy.get("cp", "")
        lieu = f"{cp} {ville}".strip() if ville or cp else ""
        adresse = sellsy.get("adresse", "")

        header = f"*{prenom} {client}*" if prenom else f"*{client}*"
        if lieu:
            header += f" — {lieu}"

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"📝 Note chantier — {client}"}
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": (
                    f"{header}\n"
                    f"📋 {nom_opp}\n"
                    f"📍 {adresse}\n\n"
                    f"*{author}* a noté :\n"
                    f">{note_text}"
                )}
            },
        ]

        # Ajouter un récap des notes précédentes s'il y en a
        notes_suivi = ch.get("notes_suivi", [])
        if len(notes_suivi) > 1:
            previous = notes_suivi[:-1][-3:]  # 3 dernières notes avant celle-ci
            recap_lines = []
            for n in previous:
                recap_lines.append(f"• _{n['par']}_ : {n['texte'][:80]}")
            if recap_lines:
                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Notes précédentes :*\n" + "\n".join(recap_lines)}
                })

        requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=5)
    except Exception as e:
        logger.warning(f"Erreur envoi Slack note : {e}")


@app.post("/chantier/{chantier_id}/note")
async def add_note(request: Request, chantier_id: str):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)

    form = await request.form()
    texte = form.get("texte", "").strip()
    if not texte:
        return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)

    chantiers = load_chantiers()
    ch = chantiers.get(chantier_id)
    if not ch:
        raise HTTPException(404)

    if "notes_suivi" not in ch:
        ch["notes_suivi"] = []

    ch["notes_suivi"].append({
        "texte": texte,
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    ch["historique"].append({
        "action": f"Note ajoutée : {texte[:50]}{'...' if len(texte) > 50 else ''}",
        "par": user["name"],
        "date": datetime.now().isoformat(),
    })

    save_chantiers(chantiers)

    # Notifier Slack en arrière-plan
    threading.Thread(target=_send_slack_note, args=(ch, texte, user["name"]), daemon=True).start()

    return RedirectResponse(f"/chantier/{chantier_id}", status_code=302)


@app.post("/sync")
async def sync(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    # Sync en arrière-plan
    if not _sync_running:
        with _sync_lock:
            if not _sync_running:
                thread = threading.Thread(target=_background_sync, daemon=True)
                thread.start()
    return RedirectResponse("/board", status_code=302)


@app.get("/api/sync")
async def api_sync(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401)
    result = sync_from_sellsy()
    return JSONResponse(result)
