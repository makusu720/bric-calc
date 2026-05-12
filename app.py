from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
import uuid
from typing import Annotated

import httpx
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from models import (
    Color,
    Part,
    SearchSession,
    STATUS_DONE,
    STATUS_ERROR,
    STATUS_PARTIAL,
    STATUS_PENDING,
    STATUS_SEARCHING,
)
from optimizer import optimize
from scrapers import (
    ALL_SCRAPERS,
    BrickOwlScraper,
    BrickwithScraper,
    MocBrickStoreScraper,
    WobrickScraper,
    load_rb_colors,
    load_rb_parts,
    rb_part_img_url,
    rb_base_part_num,
    _RB_COLOR_NAMES,
    _RB_COLOR_RGB,
    _RB_PART_NAMES,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
logger = logging.getLogger(__name__)

REBRICKABLE_API = "https://rebrickable.com/api/v3"

# Rebrickable color 9999 = [No Color/Any Color].
# Neon-Pink als visuelles Signal dass die Farbe irrelevant ist.
NO_COLOR_ID  = 9999
NO_COLOR_RGB = "FF1493"

# In-Memory Session-Store (reicht für lokale Nutzung)
sessions: dict[str, SearchSession] = {}

app = FastAPI(title="Brick Calc")


@app.on_event("startup")
async def startup_event() -> None:
    await asyncio.gather(load_rb_colors(), load_rb_parts())

templates = Jinja2Templates(directory="templates")
templates.env.filters["euro"] = lambda v: f"{v:.2f}"
templates.env.filters["euro3"] = lambda v: f"{v:.3f}"


def _part_fallback_img(part) -> str:
    """
    Gibt die Bild-URL des Basis-Teils zurück (ohne Aufdruck-Suffix),
    wenn sie sich von der Original-URL unterscheidet — sonst leer.
    """
    if not part.img_url:
        return ""
    base = rb_base_part_num(part.part_num)
    if base == part.part_num:
        return ""
    return rb_part_img_url(base, part.color.id)


templates.env.filters["fallback_img"] = _part_fallback_img


# ─── Rebrickable API-Client ────────────────────────────────────────────────────

def _normalize_id(raw: str) -> tuple[str, bool]:
    """
    Normalisiert eine MOC- oder Set-ID.
    Gibt (normalisierte_id, is_moc) zurück.
    """
    s = raw.strip()
    if re.match(r"(?i)^moc[-\s]?\d+", s):
        # Sicherstellen, dass "MOC" groß geschrieben und "-" vorhanden ist
        num = re.search(r"\d+", s).group()
        return f"MOC-{num}", True
    # Reguläres Set: z.B. "42154" → "42154-1" oder "42154-1" bleibt
    if "-" not in s:
        s = f"{s}-1"
    return s, False


async def fetch_moc_parts(api_key: str, raw_id: str) -> list[Part]:
    """Lädt die Teileliste eines MOCs oder Sets von der Rebrickable API."""
    moc_id, is_moc = _normalize_id(raw_id)
    url = (
        f"{REBRICKABLE_API}/lego/mocs/{moc_id}/parts/"
        if is_moc
        else f"{REBRICKABLE_API}/lego/sets/{moc_id}/parts/"
    )

    parts: list[Part] = []
    page = 1

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            resp = await client.get(
                url,
                headers={"Authorization": f"key {api_key}"},
                params={"page": page, "page_size": 500},
            )
            if resp.status_code == 401:
                raise ValueError(
                    "Ungültiger API Key. Bitte überprüfe deinen Rebrickable API Key unter "
                    "rebrickable.com/users/settings/#api"
                )
            if resp.status_code == 404:
                raise ValueError(
                    f"'{moc_id}' nicht gefunden. Bitte ID prüfen – "
                    "MOCs beginnen mit 'MOC-', Sets enden auf '-1' (z.B. '42154-1')."
                )
            resp.raise_for_status()

            data = resp.json()
            for item in data.get("results", []):
                if item.get("is_spare"):
                    continue  # Ersatzteile überspringen

                part_data = item.get("part", {})
                color_data = item.get("color", {})

                color_id = color_data.get("id", 0)
                parts.append(Part(
                    part_num=part_data.get("part_num", ""),
                    name=part_data.get("name", "Unbekannt"),
                    color=Color(
                        id=color_id,
                        name=color_data.get("name", "Unknown"),
                        rgb=NO_COLOR_RGB if color_id == NO_COLOR_ID else color_data.get("rgb", "888888"),
                    ),
                    quantity=item.get("quantity", 1),
                    img_url=part_data.get("part_img_url"),
                    element_id=item.get("element_id"),
                ))

            if not data.get("next"):
                break
            page += 1

    return parts


# ─── Wechselkurs ──────────────────────────────────────────────────────────────

async def fetch_exchange_rates() -> tuple[float, float]:
    """
    Holt USD→EUR und GBP→EUR von frankfurter.app.
    Gibt (usd_to_eur, gbp_to_eur) zurück.
    """
    try:
        async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
            resp = await client.get(
                "https://api.frankfurter.app/latest",
                params={"from": "EUR", "to": "USD,GBP"},
            )
            rates = resp.json()["rates"]
            # frankfurter gibt EUR→X, wir brauchen X→EUR
            usd_to_eur = 1.0 / float(rates["USD"])
            gbp_to_eur = 1.0 / float(rates["GBP"])
            logger.info("USD→EUR: %.4f  GBP→EUR: %.4f", usd_to_eur, gbp_to_eur)
            return usd_to_eur, gbp_to_eur
    except Exception as exc:
        logger.warning("Wechselkurs-Abruf fehlgeschlagen (%s), Fallback: USD 0.90 GBP 1.18", exc)
        return 0.90, 1.18


# ─── Hintergrund-Suchaufgabe ──────────────────────────────────────────────────

async def run_store_search(session_id: str) -> None:
    """Durchsucht Shops nacheinander und optimiert inkrementell nach jedem Ergebnis."""
    session = sessions.get(session_id)
    if not session:
        return

    session.status = STATUS_SEARCHING

    usd_to_eur, gbp_to_eur = await fetch_exchange_rates()
    session.usd_to_eur = usd_to_eur

    async def search_one_store(scraper_class):
        if scraper_class is WobrickScraper:
            scraper = WobrickScraper(
                usd_to_eur=usd_to_eur,
                apply_shipping_threshold=session.wobrick_shipping_threshold,
            )
        elif scraper_class is BrickwithScraper:
            scraper = BrickwithScraper(usd_to_eur=usd_to_eur)
        elif scraper_class is MocBrickStoreScraper:
            scraper = MocBrickStoreScraper(usd_to_eur=usd_to_eur)
        elif scraper_class is BrickOwlScraper:
            scraper = BrickOwlScraper(api_key=session.brickowl_api_key, gbp_to_eur=gbp_to_eur)
        else:
            scraper = scraper_class()
        session.store_progress[scraper.name] = "searching"
        try:
            async with scraper:
                results = await scraper.search_all(session.parts)
            session.store_results[scraper.name] = results
            session.store_progress[scraper.name] = "done"
            logger.info("[%s] %d Ergebnisse gefunden", scraper.name, len(results))
            return scraper
        except Exception as exc:
            logger.error("[%s] Suche fehlgeschlagen: %s", scraper.name, exc, exc_info=True)
            session.store_progress[scraper.name] = "error"
            session.store_results[scraper.name] = []
            return None

    scrapers_to_run = list(ALL_SCRAPERS)
    if session.brickowl_api_key:
        scrapers_to_run.append(BrickOwlScraper)

    tasks = {asyncio.create_task(search_one_store(cls)): cls for cls in scrapers_to_run}
    scrapers_done: list = []

    while tasks:
        done_set, pending_set = await asyncio.wait(
            tasks.keys(), return_when=asyncio.FIRST_COMPLETED
        )
        for t in done_set:
            scraper = t.result()
            if scraper is not None:
                scrapers_done.append(scraper)
            del tasks[t]

        # Nach jedem abgeschlossenen Shop neu optimieren
        try:
            store_results_strict = {
                store: [r for r in results if not r.is_alternative]
                for store, results in session.store_results.items()
            }
            session.optimize_result_strict = optimize(
                parts=session.parts,
                store_results=store_results_strict,
                scrapers=scrapers_done,
            )
            session.optimize_result = optimize(
                parts=session.parts,
                store_results=session.store_results,
                scrapers=scrapers_done,
            )
            session.result_version += 1
            if session.status != STATUS_DONE:
                session.status = STATUS_PARTIAL
        except Exception as exc:
            logger.error("Optimierung fehlgeschlagen: %s", exc, exc_info=True)

    session.status = STATUS_DONE
    session.result_version += 1


# ─── Routen ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.post("/import")
async def import_moc(
    request: Request,
    api_key: Annotated[str, Form()],
    moc_id: Annotated[str, Form()],
):
    ctx = {"api_key": api_key, "moc_id": moc_id}
    try:
        parts = await fetch_moc_parts(api_key, moc_id)
    except ValueError as exc:
        return templates.TemplateResponse(request, "index.html", {**ctx, "error": str(exc)}, status_code=400)
    except Exception as exc:
        return templates.TemplateResponse(
            request, "index.html",
            {**ctx, "error": f"Netzwerkfehler: {exc}"},
            status_code=500,
        )

    if not parts:
        return templates.TemplateResponse(
            request, "index.html",
            {**ctx, "error": "Keine Teile gefunden. Möglicherweise ist die Teileliste leer oder privat."},
            status_code=400,
        )

    session_id = str(uuid.uuid4())
    normalized_id, _ = _normalize_id(moc_id)
    sessions[session_id] = SearchSession(
        id=session_id,
        moc_id=normalized_id,
        api_key=api_key,
        parts=parts,
    )

    return RedirectResponse(f"/parts/{session_id}", status_code=303)


def _parse_rebrickable_csv(content: bytes) -> list[Part]:
    """
    Parst eine Rebrickable-CSV (Format: Part,Color,Quantity,Is Spare).
    Gibt Part-Objekte mit minimalen Infos zurück (kein Name, kein Bild).
    """
    text = content.decode("utf-8-sig").strip()  # utf-8-sig entfernt BOM falls vorhanden
    reader = csv.DictReader(io.StringIO(text))

    # Normalisiere Spaltennamen (Groß-/Kleinschreibung, Leerzeichen)
    parts: list[Part] = []
    seen: dict[tuple[str, int], Part] = {}

    for row in reader:
        # Flexibles Spalten-Mapping
        row_lower = {k.strip().lower(): v.strip() for k, v in row.items() if k}
        part_num = row_lower.get("part", "")
        color_str = row_lower.get("color", "0")
        qty_str = row_lower.get("quantity", "1")
        is_spare = row_lower.get("is spare", row_lower.get("isspare", "False"))

        if not part_num or is_spare.strip().lower() in ("true", "1", "yes"):
            continue

        try:
            color_id = int(color_str)
            quantity = int(qty_str)
        except ValueError:
            continue

        if quantity <= 0:
            continue

        key = (part_num, color_id)
        if key in seen:
            seen[key].quantity += quantity
        else:
            part = Part(
                part_num=part_num,
                name=_RB_PART_NAMES.get(part_num, part_num),
                color=Color(
                    id=color_id,
                    name=_RB_COLOR_NAMES.get(color_id, str(color_id)),
                    rgb=NO_COLOR_RGB if color_id == NO_COLOR_ID else _RB_COLOR_RGB.get(color_id, "888888"),
                ),
                quantity=quantity,
                img_url=rb_part_img_url(part_num, color_id),
            )
            seen[key] = part
            parts.append(part)

    return parts


@app.post("/upload-csv")
async def upload_csv(
    request: Request,
    csv_file: Annotated[UploadFile, File()],
):
    content = await csv_file.read()
    if not content:
        return templates.TemplateResponse(
            request, "index.html",
            {"error": "Die hochgeladene Datei ist leer.", "active_tab": "csv"},
            status_code=400,
        )

    try:
        parts = _parse_rebrickable_csv(content)
    except Exception as exc:
        return templates.TemplateResponse(
            request, "index.html",
            {"error": f"CSV konnte nicht gelesen werden: {exc}", "active_tab": "csv"},
            status_code=400,
        )

    if not parts:
        return templates.TemplateResponse(
            request, "index.html",
            {"error": "Keine Teile in der CSV gefunden. Bitte Rebrickable-Format prüfen (Part,Color,Quantity,Is Spare).", "active_tab": "csv"},
            status_code=400,
        )

    session_id = str(uuid.uuid4())
    filename = csv_file.filename or "upload.csv"
    label = filename.removesuffix(".csv")
    sessions[session_id] = SearchSession(
        id=session_id,
        moc_id=label,
        api_key="",
        parts=parts,
    )

    return RedirectResponse(f"/parts/{session_id}", status_code=303)


@app.get("/parts/{session_id}", response_class=HTMLResponse)
async def parts_page(request: Request, session_id: str):
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(404, "Session nicht gefunden – bitte neu importieren")
    return templates.TemplateResponse(request, "parts.html", {"session": session})


@app.post("/search/{session_id}")
async def start_search(
    session_id: str,
    background_tasks: BackgroundTasks,
    wobrick_shipping_threshold: Annotated[bool, Form()] = False,
    brickowl_api_key: Annotated[str, Form()] = "",
):
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(404, "Session nicht gefunden")

    # Reset für erneute Suche
    session.status = STATUS_PENDING
    session.store_results = {}
    session.store_progress = {}
    session.optimize_result = None
    session.optimize_result_strict = None
    session.result_version = 0
    session.error = None
    session.wobrick_shipping_threshold = wobrick_shipping_threshold
    session.brickowl_api_key = brickowl_api_key.strip()

    background_tasks.add_task(run_store_search, session_id)
    return RedirectResponse(f"/search/{session_id}", status_code=303)


@app.get("/search/{session_id}", response_class=HTMLResponse)
async def search_page(request: Request, session_id: str):
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(404, "Session nicht gefunden")
    if session.status in (STATUS_DONE, STATUS_PARTIAL):
        return RedirectResponse(f"/results/{session_id}")
    return templates.TemplateResponse(request, "search.html", {"session": session})


@app.get("/results/{session_id}", response_class=HTMLResponse)
async def results_page(request: Request, session_id: str):
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(404, "Session nicht gefunden")
    if session.status not in (STATUS_DONE, STATUS_PARTIAL):
        return RedirectResponse(f"/search/{session_id}")
    show_alts = request.query_params.get("alts", "0") == "1"
    result = session.optimize_result if show_alts else session.optimize_result_strict
    return templates.TemplateResponse(request, "results.html", {
        "session": session,
        "result": result,
        "show_alts": show_alts,
    })


@app.get("/download-csv/{session_id}/{store_name}")
async def download_store_csv(request: Request, session_id: str, store_name: str):
    session = sessions.get(session_id)
    if not session or session.optimize_result is None:
        raise HTTPException(404, "Session oder Ergebnis nicht gefunden")

    show_alts = request.query_params.get("alts", "0") == "1"
    active_result = session.optimize_result if show_alts else session.optimize_result_strict
    if active_result is None:
        active_result = session.optimize_result

    order = next(
        (o for o in active_result.orders if o.store_name == store_name),
        None,
    )
    if not order:
        raise HTTPException(404, f"Kein Auftrag für Shop '{store_name}' gefunden")

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Part", "Color", "Quantity", "Is Spare"])
    for line in order.lines:
        if line.is_alternative:
            continue  # Ersatzteile nicht im Shop-CSV – Originale separat über "missing" beziehen
        writer.writerow([line.part.part_num, line.part.color.id, line.quantity, "False"])

    filename = f"{session.moc_id}_{store_name}.csv".replace(" ", "_")
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/download-missing-csv/{session_id}")
async def download_missing_csv(request: Request, session_id: str):
    session = sessions.get(session_id)
    if not session or session.optimize_result is None:
        raise HTTPException(404, "Session oder Ergebnis nicht gefunden")

    show_alts = request.query_params.get("alts", "0") == "1"
    active_result = session.optimize_result if show_alts else session.optimize_result_strict
    if active_result is None:
        active_result = session.optimize_result

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Part", "Color", "Quantity", "Is Spare"])
    for part in active_result.unavailable:
        writer.writerow([part.part_num, part.color.id, part.quantity, "False"])

    filename = f"{session.moc_id}_missing.csv".replace(" ", "_")
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/status/{session_id}")
async def api_status(session_id: str):
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(404)
    return {
        "status": session.status,
        "progress": session.store_progress,
        "done": session.status in (STATUS_DONE, STATUS_ERROR),
        "error": session.error,
        "result_version": session.result_version,
    }


@app.get("/api/results-fragment/{session_id}")
async def results_fragment(request: Request, session_id: str, alts: str = "0"):
    session = sessions.get(session_id)
    if not session:
        raise HTTPException(404)
    show_alts = alts == "1"
    result = session.optimize_result if show_alts else session.optimize_result_strict
    if result is None:
        return HTMLResponse("", status_code=204)
    return templates.TemplateResponse(request, "_results_content.html", {
        "session": session,
        "result": result,
        "show_alts": show_alts,
    })
