"""
Shop-Scraper via Rebrickable-CSV-Upload.

brickwith: vollständig implementiert (4-Schritt Alibaba-OSS-Flow, USD)
wobrick:   vollständig implementiert (WordPress AJAX, USD)
SNAP:      vollständig implementiert (curl-cffi Cloudflare-Bypass, EUR)
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import io
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from models import Part, StoreResult

logger = logging.getLogger(__name__)

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}


def generate_rebrickable_csv(parts: list[Part]) -> bytes:
    """
    Erzeugt eine Rebrickable-kompatible CSV aus der Teileliste.

    Format:
        Part,Color,Quantity,Is Spare
        3001,4,2,False
    """
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Part", "Color", "Quantity", "Is Spare"])
    for part in parts:
        # Rebrickable-Sonderfarbe 9999 = [No Color/Any Color] → 0 (Not Applicable)
        # Shops erkennen 9999 nicht; 0 ist der universelle "keine Farbe"-Wert.
        color_id = 0 if part.color.id == 9999 else part.color.id
        writer.writerow([part.part_num, color_id, part.quantity, "False"])
    return buf.getvalue().encode("utf-8")


def _parse_price(text: str) -> Optional[float]:
    text = text.replace("\xa0", " ").strip()
    match = re.search(r"[\d]+[.,][\d]+", text)
    if not match:
        return None
    return float(match.group().replace(",", "."))


def _parse_stock(text: str) -> int:
    match = re.search(r"\d+", text.strip())
    return int(match.group()) if match else 0


# ─────────────────────────────────────────────────────────────────────────────
# Basisklasse
# ─────────────────────────────────────────────────────────────────────────────

class BaseScraper(ABC):
    name: str
    base_url: str
    shipping_cost: float = 3.99
    shipping_free_threshold: float = 0.0

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "BaseScraper":
        self._client = httpx.AsyncClient(
            headers=_BROWSER_HEADERS,
            timeout=60.0,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()

    @abstractmethod
    async def search_all(self, parts: list[Part]) -> list[StoreResult]:
        """Sucht alle Teile und gibt Ergebnisse zurück."""
        ...

    def calculate_shipping(self, order_value: float) -> float:
        if self.shipping_free_threshold > 0 and order_value >= self.shipping_free_threshold:
            return 0.0
        return self.shipping_cost


# ─────────────────────────────────────────────────────────────────────────────
# brickwith  (vollständig implementiert)
# ─────────────────────────────────────────────────────────────────────────────

class BrickwithScraper(BaseScraper):
    """
    brickwith-Scraper via Alibaba-OSS-Upload-Flow.

    4 Schritte:
      1. OSS-Credentials vom brickwith-Server holen
      2. CSV direkt zu Alibaba Cloud OSS hochladen
      3. Datei-Metadaten bei brickwith registrieren
      4. Parts parsen lassen und JSON-Antwort auswerten

    Konfiguration:
      customer_id  – optional; brickwith-Kunden-ID aus deinem Account.
                     Leer lassen für anonymen Zugriff (kann je nach API-Stand
                     funktionieren oder nicht).
                     Zu finden: brickwith.com → DevTools → Application →
                     Local Storage → Schlüssel mit 'customer' o.Ä.
    """

    name = "brickwith"
    currency = "USD"
    base_url = "https://www.brickwith.com"
    api_base = "https://beta-server.brickwith.com"

    # Öffentlicher API-Key aus dem brickwith-Frontend (für OSS-STS-Endpoint)
    _OSS_API_KEY = "cHXaWZ04X8EgMRtFMMfwrzizvRl6kfC4Bw8p"

    # Bild-Basis-URL – anpassen falls Bilder nicht laden
    _IMG_BASE = "https://www.brickwith.com"

    _SHIPPING_COST_USD: float = 3.32   # USD (laut brickwith.com)
    shipping_free_threshold = 0.0

    # Optional: eigene customer_id eintragen (leer = anonymer Upload)
    customer_id: str = ""

    def __init__(self, usd_to_eur: float = 0.90) -> None:
        super().__init__()
        self._usd_to_eur = usd_to_eur
        self.shipping_cost = self._SHIPPING_COST_USD * usd_to_eur  # → EUR

    async def search_all(self, parts: list[Part]) -> list[StoreResult]:
        csv_bytes = generate_rebrickable_csv(parts)

        # Dateiname im brickwith-Format: YYYYMMDDHHmmss000.csv
        filename = datetime.now().strftime("%Y%m%d%H%M%S") + "000.csv"
        raw_filename = f"rebrickable_parts_{len(parts)}.csv"
        dir_name = "upload_partlist_file"
        relative_path = f"{dir_name}/{filename}"

        logger.info("[brickwith] Schritt 1: OSS-Credentials holen")
        sts = await self._get_oss_sts(filename, len(csv_bytes), dir_name)

        logger.info("[brickwith] Schritt 2: CSV zu Alibaba OSS hochladen")
        await self._upload_to_oss(sts, relative_path, csv_bytes)

        logger.info("[brickwith] Schritt 3: Datei registrieren")
        file_id = await self._register_file(filename, raw_filename, relative_path, len(csv_bytes))

        logger.info("[brickwith] Schritt 4: Parts parsen (id=%s)", file_id)
        return await self._parse_file(file_id, parts)

    async def _get_oss_sts(self, filename: str, size: int, dir_name: str) -> dict:
        assert self._client is not None
        uid = f"rc-upload-{int(datetime.now().timestamp() * 1000)}-1"
        resp = await self._client.post(
            f"{self.api_base}/medusa_api/store_v2/oss/get_oss_sts",
            headers={
                "Authorization": self._OSS_API_KEY,
                "Content-Type": "application/json",
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/",
            },
            json={
                "file": {"uid": uid},
                "size": size,
                "newFileName": filename,
                "suffix": ".csv",
                "updateType": "",
                "rawFileName": filename,
                "appCategory": "WebPartListUpload",
                "dir": dir_name,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 1:
            raise RuntimeError(f"[brickwith] OSS-STS fehlgeschlagen: {data.get('msg')}")
        return data["data"]

    async def _upload_to_oss(self, sts: dict, key: str, csv_bytes: bytes) -> None:
        assert self._client is not None
        resp = await self._client.post(
            sts["host"],
            data={
                "OSSAccessKeyId": sts["accessid"],
                "policy": sts["policy"],
                "Signature": sts["signature"],
                "key": key,
                "success_action_status": "200",
                "Content-Type": "text/csv",
            },
            files={"file": ("file.csv", csv_bytes, "text/csv")},
        )
        if resp.status_code not in (200, 204):
            raise RuntimeError(f"[brickwith] OSS-Upload fehlgeschlagen: HTTP {resp.status_code}")
        logger.info("[brickwith] OSS-Upload erfolgreich")

    async def _register_file(
        self,
        filename: str,
        raw_filename: str,
        relative_path: str,
        size: int,
    ) -> str:
        assert self._client is not None
        body: dict = {
            "file_name": filename,
            "size": size,
            "relative_path": relative_path,
            "suffix": ".csv",
            "app_category": "WebPartListUpload",
            "raw_file_name": raw_filename,
            "metadata": {"update_type": ""},
        }
        if self.customer_id:
            body["customer_id"] = self.customer_id

        resp = await self._client.post(
            f"{self.api_base}/medusa_api/open/file_info_create",
            headers={
                "Content-Type": "application/json",
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/",
            },
            json=body,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 1:
            raise RuntimeError(f"[brickwith] file_info_create fehlgeschlagen: {data.get('msg')}")
        return data["data"]["id"]

    async def _parse_file(self, file_id: str, parts: list[Part]) -> list[StoreResult]:
        assert self._client is not None
        resp = await self._client.post(
            f"{self.api_base}/main_api/store_v2/part_list/parse_file",
            headers={
                "Content-Type": "application/json",
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/",
            },
            json={"id": file_id, "need_parse": True},
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 1:
            raise RuntimeError(f"[brickwith] parse_file fehlgeschlagen: {data.get('msg')}")
        return self._extract_results(data, parts)

    def _extract_results(self, data: dict, parts: list[Part]) -> list[StoreResult]:
        # Schneller Lookup: (rebrickable_id, rebrickable_color_id) → Part
        # Farbe 9999 = [No Color/Any Color] wird als 0 in der CSV gesendet → auch 0 mappen
        parts_lookup: dict[tuple[str, str], Part] = {
            (p.part_num, str(p.color.id)): p for p in parts
        }
        for p in parts:
            if p.color.id == 9999:
                parts_lookup[(p.part_num, "0")] = p

        results: list[StoreResult] = []

        for item in data.get("partList", []):
            if not item.get("is_available"):
                continue

            price = item.get("price", 0.0)
            if not price or price <= 0:
                continue

            rb_id = str(item.get("rebrickable_id", ""))
            rb_color = str(item.get("rebrickable_color_id", ""))
            part = parts_lookup.get((rb_id, rb_color))
            if part is None:
                continue

            sku = item.get("sku_code", "")
            # Die API gibt quantity = benötigte Menge zurück, kein expliziter Lagerbestand.
            # Wir setzen stock = benötigte Menge (d.h. "ausreichend verfügbar").
            quantity_needed = part.quantity

            img_rel = item.get("img_url", "")
            img_url = f"{self._IMG_BASE}/{img_rel}" if img_rel else ""

            results.append(StoreResult(
                store_name=self.name,
                part_num=rb_id,
                color_id=part.color.id,
                color_name=item.get("color_name", part.color.name),
                unit_price=float(price) * self._usd_to_eur,  # USD → EUR
                stock=9999,   # brickwith meldet nur Verfügbarkeit, keinen genauen Bestand
                part_url=f"{self.base_url}/products/{sku}" if sku else self.base_url,
            ))

        logger.info(
            "[brickwith] %d/%d Teile verfügbar",
            len(results),
            len(data.get("partList", [])),
        )
        return results


# ─────────────────────────────────────────────────────────────────────────────
# SNAP  (curl-cffi für Cloudflare-Bypass)
# ─────────────────────────────────────────────────────────────────────────────

class SNAPScraper(BaseScraper):
    """
    Scraper für snap.co via Rebrickable-CSV-Upload.

    Benutzt curl-cffi mit Chrome-TLS-Fingerprint um Cloudflare zu umgehen.

    Ablauf:
      1. GET /en/parts/upload  → CSRF-Token (_token) extrahieren
      2. POST /en/parts/process-partlist mit file + _token
      3. HTML-Antwort parsen: Cards mit data-mapping-type="available" oder
         data-mapping-type="alternative" und data-selected="true"

    Versandkosten: TODO auf snap.co nachschauen und anpassen.
    """

    name = "SNAP"
    currency = "EUR"
    base_url = "https://snap.co"
    shipping_cost = 4.95
    shipping_free_threshold = 75.0

    _UPLOAD_PAGE = "https://snap.co/en/parts/upload"
    _PROCESS_URL = "https://snap.co/en/parts/process-partlist"

    def __init__(self) -> None:
        self._pw_ctx = None
        self._browser = None

    async def __aenter__(self) -> "SNAPScraper":
        from playwright.async_api import async_playwright
        self._pw_ctx = async_playwright()
        pw = await self._pw_ctx.__aenter__()
        self._browser = await pw.chromium.launch(headless=True)
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._browser:
            await self._browser.close()
        if self._pw_ctx:
            await self._pw_ctx.__aexit__(*args)

    async def search_all(self, parts: list[Part]) -> list[StoreResult]:
        assert self._browser is not None
        csv_bytes = generate_rebrickable_csv(parts)

        context = await self._browser.new_context()
        page = await context.new_page()
        try:
            # Upload-Seite laden; der Browser führt JS aus → XSRF-Token wird gesetzt
            logger.info("[SNAP] Upload-Seite laden")
            await page.goto(self._UPLOAD_PAGE, wait_until="networkidle", timeout=30_000)

            # Datei-Input setzen und gleichzeitig die Netzwerkantwort abfangen
            logger.info("[SNAP] CSV hochladen")
            file_input = page.locator('input[type="file"]')
            await file_input.wait_for(state="attached", timeout=15_000)

            async with page.expect_response(
                lambda r: "process-partlist" in r.url and r.status == 200,
                timeout=60_000,
            ) as response_info:
                await file_input.set_input_files({
                    "name": "rebrickable_parts.csv",
                    "mimeType": "text/csv",
                    "buffer": csv_bytes,
                })

            html = await (await response_info.value).text()
        finally:
            await context.close()

        results = self._parse_html(html, parts)
        logger.info("[SNAP] %d Teile gefunden", len(results))
        return results

    def _parse_html(self, html: str, parts: list[Part]) -> list[StoreResult]:
        """
        Parst die SNAP-Ergebnis-HTML (Fragment vom AJAX-Endpoint).

        Struktur:
          - Direkt verfügbare Teile: .elementpicker-card[data-mapping-type="available"]
            → element-color-name enthält "{part_num}-{color_id}"
          - Alternativen: .alternativepicker Zeilen, je mit
            · einer Original-Karte (kein mapping-type) → liefert part_num + color_id
            · mehreren Alternative-Karten; die mit data-selected="true" wird genommen
              → liefert Preis (die Karte selbst hat leeres element-color-name!)
        """
        soup = BeautifulSoup(html, "lxml")

        # Farbe 9999 = [No Color/Any Color] wird als 0 in der CSV gesendet → auch 0 mappen
        parts_lookup: dict[tuple[str, int], Part] = {
            (p.part_num, p.color.id): p for p in parts
        }
        for p in parts:
            if p.color.id == 9999:
                parts_lookup[(p.part_num, 0)] = p
        results: list[StoreResult] = []
        seen: set[tuple[str, int]] = set()

        def _parse_id(raw: str) -> Optional[tuple[str, int]]:
            raw = raw.strip()
            if not raw:
                return None
            last_dash = raw.rfind("-")
            if last_dash == -1:
                return None
            try:
                return raw[:last_dash], int(raw[last_dash + 1:])
            except ValueError:
                return None

        def _add(
            part_num: str,
            color_id: int,
            price: float,
            product_id: str,
            is_alternative: bool = False,
            alt_part_num: str = "",
            alt_color_id: int = 0,
            alt_color_name: str = "",
            alt_color_rgb: str = "",
        ) -> None:
            key = (part_num, color_id)
            if key in seen:
                return
            part = parts_lookup.get(key)
            if part is None:
                return
            seen.add(key)
            part_url = (
                f"{self.base_url}/en/products/{product_id}"
                if product_id else self.base_url
            )
            results.append(StoreResult(
                store_name=self.name,
                part_num=part_num,
                color_id=color_id,
                color_name=part.color.name,
                unit_price=price,
                stock=9999,   # SNAP gibt keinen Lagerbestand aus → hohen Wert setzen
                part_url=part_url,
                is_alternative=is_alternative,
                alt_part_num=alt_part_num,
                alt_color_id=alt_color_id,
                alt_color_name=alt_color_name,
                alt_color_rgb=alt_color_rgb,
            ))

        # 1. Direkt verfügbare Teile
        for card in soup.select('.elementpicker-card[data-mapping-type="available"]'):
            try:
                raw_id = card.select_one(".element-color-name")
                parsed = _parse_id(raw_id.get_text() if raw_id else "")
                if not parsed:
                    continue
                price = _parse_price(card.get("data-element-price", ""))
                if price and price > 0:
                    _add(*parsed, price, card.get("data-product-id", ""))
            except Exception as exc:
                logger.debug("[SNAP] Parse-Fehler (available): %s", exc)

        # 2. Shop-Alternativen (andere Farbe oder ohne Druck)
        # Jede .alternativepicker-Zeile hat links die Original-Karte (liefert IDs)
        # und rechts die Alternativkarten (liefern Preis + product-id).
        # Diese werden als is_alternative=True markiert, damit der Optimizer
        # sie in der strengen Berechnung (nur exakte Treffer) ignorieren kann.
        for row in soup.select(".alternativepicker"):
            try:
                orig = row.select_one(
                    ".elementpicker-card:not(.element-alternative-picker-card)"
                )
                if not orig:
                    continue
                raw_id = orig.select_one(".element-color-name")
                parsed = _parse_id(raw_id.get_text() if raw_id else "")
                if not parsed:
                    continue

                alt = row.select_one(
                    '.elementpicker-card[data-mapping-type="alternative"][data-selected="true"]'
                )
                if not alt:
                    continue
                price = _parse_price(alt.get("data-element-price", ""))
                if price and price > 0:
                    # Ersatzteil-Infos aus der Alternativ-Karte lesen
                    alt_name_el = alt.select_one(".element-color-name")
                    alt_parsed = _parse_id(alt_name_el.get_text() if alt_name_el else "")
                    alt_pn = alt_parsed[0] if alt_parsed else ""
                    alt_cid = alt_parsed[1] if alt_parsed else 0
                    alt_cname = _RB_COLOR_NAMES.get(alt_cid, "") if alt_cid else ""
                    alt_crgb = _RB_COLOR_RGB.get(alt_cid, "") if alt_cid else ""
                    _add(
                        *parsed,
                        price,
                        alt.get("data-product-id", ""),
                        is_alternative=True,
                        alt_part_num=alt_pn,
                        alt_color_id=alt_cid,
                        alt_color_name=alt_cname,
                        alt_color_rgb=alt_crgb,
                    )
            except Exception as exc:
                logger.debug("[SNAP] Parse-Fehler (alternative): %s", exc)

        return results


# ─────────────────────────────────────────────────────────────────────────────
# wobrick
# ─────────────────────────────────────────────────────────────────────────────

class WobrickScraper(BaseScraper):
    """
    Scraper für wobrick via WordPress AJAX-Endpoint.

    Einziger POST an /wp-admin/admin-ajax.php mit den Feldern
    action=bulk_order, fn=bulk_order_upload, upload=<csv>.
    Antwort ist JSON mit itemList (gefunden) und missList (nicht gefunden).
    Preise sind in USD → werden mit usd_to_eur umgerechnet.

    Versandkosten: $3.00 USD, Erstattung per Mail wenn Bestellwert > $20 innerhalb 3 Tagen.
    Mit apply_shipping_threshold=True (Standard) berücksichtigt der Optimizer die Freigrenze.
    Auf False setzen um immer $3.00 Versand einzukalkulieren.
    """

    name = "wobrick"
    currency = "USD"
    base_url = "https://wobrick.com"

    # USD-Rohwerte – werden in __init__ nach EUR umgerechnet
    _SHIPPING_COST_USD: float = 3.00
    _SHIPPING_FREE_THRESHOLD_USD: float = 20.00

    def __init__(self, usd_to_eur: float = 0.90, apply_shipping_threshold: bool = True) -> None:
        super().__init__()
        self._usd_to_eur = usd_to_eur
        # Versandkosten und Freigrenze in EUR umrechnen (Optimizer arbeitet in EUR)
        self.shipping_cost = self._SHIPPING_COST_USD * usd_to_eur
        self.shipping_free_threshold = (
            self._SHIPPING_FREE_THRESHOLD_USD * usd_to_eur
            if apply_shipping_threshold
            else 0.0
        )

    async def search_all(self, parts: list[Part]) -> list[StoreResult]:
        csv_bytes = generate_rebrickable_csv(parts)
        assert self._client is not None

        resp = await self._client.post(
            f"{self.base_url}/wp-admin/admin-ajax.php",
            headers={
                "Accept": "*/*",
                "Origin": self.base_url,
                "Referer": f"{self.base_url}/toolkit",
                "X-Requested-With": "XMLHttpRequest",
            },
            data={
                "action": "bulk_order",
                "fn": "bulk_order_upload",
            },
            files={
                "upload": ("rebrickable_parts.csv", csv_bytes, "text/csv"),
            },
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            raise RuntimeError(f"[wobrick] Upload fehlgeschlagen: {data}")

        results = self._extract_results(data["data"], parts)
        logger.info("[wobrick] %d Teile gefunden", len(results))
        return results

    def _extract_results(self, data: dict, parts: list[Part]) -> list[StoreResult]:
        # Lookup: (designid, colorid) → Part
        # Farbe 9999 = [No Color/Any Color] wird als 0 in der CSV gesendet → auch 0 mappen
        parts_lookup: dict[tuple[str, str], Part] = {
            (p.part_num, str(p.color.id)): p for p in parts
        }
        for p in parts:
            if p.color.id == 9999:
                parts_lookup[(p.part_num, "0")] = p

        results: list[StoreResult] = []

        for item in data.get("itemList", []):
            info = item.get("info")
            if not info:
                continue

            design_id = str(item.get("designid", ""))
            color_id  = str(item.get("colorid", ""))
            part = parts_lookup.get((design_id, color_id))
            if part is None:
                continue

            try:
                price_usd = float(info["price"])
            except (KeyError, ValueError, TypeError):
                continue

            stock = int(info.get("stock", 0))
            if stock == 0:
                continue

            results.append(StoreResult(
                store_name=self.name,
                part_num=design_id,
                color_id=part.color.id,
                color_name=part.color.name,
                unit_price=price_usd * self._usd_to_eur,   # USD → EUR
                stock=stock,
                part_url=info.get("url", self.base_url),
            ))

        return results


# ─────────────────────────────────────────────────────────────────────────────
# BrickOwl  (offizielle API)
# ─────────────────────────────────────────────────────────────────────────────


# Rebrickable color-ID → (name, rgb).
# Wird einmalig beim App-Start von rebrickable.com geladen (öffentlich, kein API-Key).
_RB_COLOR_NAMES: dict[int, str] = {}
_RB_COLOR_RGB:   dict[int, str] = {}   # hex ohne #, z.B. "C91A09"
_RB_PART_NAMES:  dict[str, str] = {}   # part_num → name
_rb_colors_loaded = False
_rb_parts_loaded  = False

_RB_CDN = "https://cdn.rebrickable.com/media"


def rb_part_img_url(part_num: str, color_id: int) -> str:
    """Konstruiert die öffentliche Rebrickable-Bild-URL für ein Teil in einer Farbe."""
    return f"{_RB_CDN}/parts/ldraw/{color_id}/{part_num}.png"


def rb_base_part_num(part_num: str) -> str:
    """
    Gibt die Basis-Teilenummer ohne Aufdruck-/Muster-Suffix zurück.
    Beispiele:
      98138pr0060 → 98138
      3626cpb2456 → 3626c
      973pr1234   → 973
    Gibt die Original-Nummer zurück wenn kein Suffix erkannt wird.
    """
    base = re.sub(r"(pr|pb|pat)[a-z0-9]*$", "", part_num, flags=re.IGNORECASE)
    return base if base and base != part_num else part_num


async def _fetch_gz_csv(url: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
    data = gzip.decompress(resp.content).decode("utf-8")
    return list(csv.DictReader(io.StringIO(data)))


async def load_rb_colors() -> None:
    """Lädt colors.csv.gz → _RB_COLOR_NAMES / _RB_COLOR_RGB. Idempotent."""
    global _RB_COLOR_NAMES, _RB_COLOR_RGB, _rb_colors_loaded
    if _rb_colors_loaded:
        return
    try:
        rows = await _fetch_gz_csv(f"{_RB_CDN}/downloads/colors.csv.gz")
        for row in rows:
            try:
                cid = int(row["id"])
                _RB_COLOR_NAMES[cid] = row["name"]
                _RB_COLOR_RGB[cid]   = row["rgb"].lstrip("#").upper()
            except (KeyError, ValueError):
                pass
        logger.info("Rebrickable-Farben: %d geladen", len(_RB_COLOR_NAMES))
    except Exception as exc:
        logger.warning("Rebrickable-Farben konnten nicht geladen werden: %s", exc)
    finally:
        _rb_colors_loaded = True


async def load_rb_parts() -> None:
    """Lädt parts.csv.gz → _RB_PART_NAMES. Idempotent."""
    global _RB_PART_NAMES, _rb_parts_loaded
    if _rb_parts_loaded:
        return
    try:
        rows = await _fetch_gz_csv(f"{_RB_CDN}/downloads/parts.csv.gz")
        for row in rows:
            pn = row.get("part_num", "").strip()
            name = row.get("name", "").strip()
            if pn and name:
                _RB_PART_NAMES[pn] = name
        logger.info("Rebrickable-Teile: %d geladen", len(_RB_PART_NAMES))
    except Exception as exc:
        logger.warning("Rebrickable-Teile konnten nicht geladen werden: %s", exc)
    finally:
        _rb_parts_loaded = True


class BrickOwlScraper(BaseScraper):
    """
    BrickOwl-Marketplace via offizielle REST-API.

    Ablauf:
      1. Einmalig: /v1/catalog/color_list → Farbname-Map (rb_id falls vorhanden)
      2. Je Teil: /v1/catalog/id_lookup mit design_id → BOID(s)
      3. Je BOID: /v1/catalog/availability_basic → günstigster Preis in GBP

    Versandkosten werden nicht einkalkuliert (Marketplace, Versand je nach Seller).
    Preise werden mit gbp_to_eur-Kurs nach EUR umgerechnet.

    Benötigt "Catalog Approval" im BrickOwl-Account für Availability-Endpoints.
    """

    name = "BrickOwl"
    currency = "GBP"
    base_url = "https://www.brickowl.com"
    shipping_cost = 0.0
    shipping_free_threshold = 0.0

    _API_BASE = "https://api.brickowl.com/v1"
    _CONCURRENCY = 5

    def __init__(self, api_key: str, gbp_to_eur: float = 1.18) -> None:
        super().__init__()
        self._api_key = api_key
        self._gbp_to_eur = gbp_to_eur
        # rb_color_id → bw_color_id  (befüllt durch _build_color_map)
        self._rb_to_bw: dict[int, int] = {}
        # normierter Farbname → bw_color_id  (Fallback)
        self._name_to_bw: dict[str, int] = {}

    @staticmethod
    def _norm_color(s: str) -> str:
        return s.lower().replace(" ", "").replace("-", "").replace("_", "")

    async def search_all(self, parts: list[Part]) -> list[StoreResult]:
        # Farbkarte aufbauen: rb_color_id → bw_color_id
        await self._build_color_map()

        sem = asyncio.Semaphore(self._CONCURRENCY)

        async def lookup(part: Part) -> list[StoreResult]:
            async with sem:
                try:
                    return await self._search_part(part)
                except Exception as exc:
                    logger.warning("[BrickOwl] %s Farbe %d: %s", part.part_num, part.color.id, exc)
                    return []

        nested = await asyncio.gather(*[lookup(p) for p in parts])
        results = [r for sub in nested for r in sub]
        logger.info("[BrickOwl] %d/%d Teile gefunden", len(results), len(parts))
        return results

    async def _build_color_map(self) -> None:
        """Lädt Rebrickable-Farbtabelle + BrickOwl color_list und baut Name-Map."""
        assert self._client is not None
        await load_rb_colors()
        try:
            resp = await self._client.get(
                f"{self._API_BASE}/catalog/color_list",
                params={"key": self._api_key},
            )
            resp.raise_for_status()
            colors = resp.json()
        except Exception as exc:
            logger.error("[BrickOwl] color_list fehlgeschlagen: %s", exc)
            return

        # Antwort ist ein Dict: {"0": {"id": "0", "name": "Not Applicable", ...}, "2": {...}}
        entries = colors.values() if isinstance(colors, dict) else colors
        for c in entries:
            bw_id = c.get("id")
            name  = str(c.get("name") or "")
            if bw_id is not None and name:
                self._name_to_bw[self._norm_color(name)] = int(bw_id)

        logger.info("[BrickOwl] color_list: %d Farben geladen", len(self._name_to_bw))

    async def _search_part(self, part: Part) -> list[StoreResult]:
        """
        Direkt via availability_basic — kein id_lookup nötig.

        BOID-Format: "{design_id}-{bw_color_id}" (z.B. "474589-101").
        Wir konstruieren den BOID aus der Farbkarte und rufen
        availability_basic direkt damit auf.
        """
        assert self._client is not None

        # BrickOwl-Farb-ID bestimmen.
        # Sonderfall: Rebrickable 9999 = [No Color/Any Color] → BW 0 = Not Applicable
        if part.color.id == 9999:
            bw_color_id: Optional[int] = 0
        else:
            # Bei CSV-Importen ist color.name nur die numerische ID (z.B. "70") —
            # dann über die statische Rebrickable-Farbtabelle den richtigen Namen suchen.
            color_name = part.color.name
            if color_name.strip().lstrip("-").isdigit():
                color_name = _RB_COLOR_NAMES.get(part.color.id, color_name)

            bw_color_id: Optional[int] = (
                self._rb_to_bw.get(part.color.id)
                or self._name_to_bw.get(self._norm_color(color_name))
            )
        if bw_color_id is None:
            logger.debug(
                "[BrickOwl] Farbe nicht gefunden: rb_id=%d name='%s' → lookup='%s'",
                part.color.id, part.color.name, color_name,
            )
            return []

        # BOID direkt konstruieren: "{design_id}-{bw_color_id}"
        boid = f"{part.part_num}-{bw_color_id}"

        # availability_basic → günstigster Preis in GBP
        resp = await self._client.get(
            f"{self._API_BASE}/catalog/availability_basic",
            params={
                "key":     self._api_key,
                "boid":    boid,
                "country": "DE",
            },
        )
        if resp.status_code in (204, 404):
            return []
        if resp.status_code == 403:
            raise RuntimeError(
                "BrickOwl: Catalog Approval fehlt. "
                "Bitte in den BrickOwl-Account-Einstellungen unter API → Catalog Access anfragen."
            )
        resp.raise_for_status()

        try:
            avail = resp.json()
        except Exception:
            return []

        logger.debug("[BrickOwl] availability_basic %s: %s", boid, avail)

        if isinstance(avail, list):
            avail = avail[0] if avail else {}

        price_gbp = (
            avail.get("min_price")
            or avail.get("price")
            or avail.get("cheapest_price")
        )
        if not price_gbp:
            return []

        price_eur = float(price_gbp) * self._gbp_to_eur
        stock = int(avail.get("quantity") or avail.get("stock") or part.quantity)

        return [StoreResult(
            store_name=self.name,
            part_num=part.part_num,
            color_id=part.color.id,
            color_name=part.color.name,
            unit_price=price_eur,
            stock=stock,
            part_url=f"{self.base_url}/catalog/{boid}",
        )]


# ─────────────────────────────────────────────────────────────────────────────
# MocBrickStore / Gobricks  (GraphQL-API)
# ─────────────────────────────────────────────────────────────────────────────

class MocBrickStoreScraper(BaseScraper):
    """
    Scraper für toolbox.mocbrickstore.com (Gobricks-Teile via GraphQL-API).

    Ablauf:
      1. Alle Teile als Strings "{part_num}-{io_colour_id}-{quantity}" formatieren
         (io_colour_id = Rebrickable-Farb-ID; 9999 → 0)
      2. Einzelner POST an die Cloud-Function-API
      3. gobricks-Einträge auswerten: lego_id + io_colour_id → Part matchen

    Einschränkungen:
      - Kein Lagerbestand in der API → stock=9999 (Verfügbarkeit angenommen)
      - Gobricks-Bestellsystem: max. 200 verschiedene Teilenummern pro Bestellung
      - Versandkosten: $3,99 USD, kostenlos ab $20 USD
    """

    name = "MocBrickStore"
    currency = "USD"
    base_url = "https://toolbox.mocbrickstore.com"
    max_order_types: int = 200  # Gobricks-Limit: max. 200 Teiletypen pro Bestellung

    _API_URL = "https://asia-southeast1-mocbrickstore-446d4.cloudfunctions.net/api"
    _SHIPPING_COST_USD: float = 3.99
    _SHIPPING_FREE_THRESHOLD_USD: float = 20.00

    _GQL = (
        "query GetGobricksWithRbCsvParts("
        "$parts: [String!], $colorType: String, $page: Int!, $limit: Int!) {"
        "  GetGobricksWithRbCsvParts("
        "    parts: $parts colorType: $colorType page: $page limit: $limit) {"
        "    notFoundParts"
        "    gobricks {"
        "      lego_id io_colour_id price image_src handle"
        "      rebrickable_colour_name quantity __typename"
        "    }"
        "    __typename"
        "  }"
        "}"
    )

    def __init__(self, usd_to_eur: float = 0.90) -> None:
        super().__init__()
        self._usd_to_eur = usd_to_eur
        self.shipping_cost = self._SHIPPING_COST_USD * usd_to_eur
        self.shipping_free_threshold = self._SHIPPING_FREE_THRESHOLD_USD * usd_to_eur

    async def search_all(self, parts: list[Part]) -> list[StoreResult]:
        assert self._client is not None

        # "{part_num}-{io_colour_id}-{quantity}"
        # io_colour_id = Rebrickable-Farb-ID (9999 → 0)
        part_strings = [
            f"{p.part_num}-{0 if p.color.id == 9999 else p.color.id}-{p.quantity}"
            for p in parts
        ]
        parts_lookup: dict[tuple[str, int], Part] = {
            (p.part_num, 0 if p.color.id == 9999 else p.color.id): p
            for p in parts
        }

        resp = await self._client.post(
            self._API_URL,
            headers={
                "accept": "*/*",
                "content-type": "application/json",
                "origin": self.base_url,
                "referer": f"{self.base_url}/",
                "authorization": "",
            },
            json={
                "operationName": "GetGobricksWithRbCsvParts",
                "variables": {
                    "parts": part_strings,
                    "colorType": "io",
                    "page": 1,
                    "limit": 5000,
                },
                "query": self._GQL,
            },
            timeout=90.0,
        )
        resp.raise_for_status()
        data = resp.json()

        gobricks = (
            data.get("data", {})
            .get("GetGobricksWithRbCsvParts", {})
            .get("gobricks", [])
        )

        results: list[StoreResult] = []
        seen: set[tuple[str, int]] = set()

        for item in gobricks:
            lego_id = str(item.get("lego_id") or "").strip()
            if not lego_id or lego_id.lower() == "nan":
                # Kein Rebrickable-Match möglich → überspringen
                continue

            try:
                io_color_id = int(item.get("io_colour_id") or 0)
                price_usd = float(item.get("price") or 0)
            except (ValueError, TypeError):
                continue

            if price_usd <= 0:
                continue

            key = (lego_id, io_color_id)
            if key in seen:
                continue
            part = parts_lookup.get(key)
            if part is None:
                continue
            seen.add(key)

            handle = item.get("handle", "")
            part_url = (
                f"https://www.gobricks.cn/products/{handle}"
                if handle else self.base_url
            )
            results.append(StoreResult(
                store_name=self.name,
                part_num=lego_id,
                color_id=part.color.id,
                color_name=item.get("rebrickable_colour_name") or part.color.name,
                unit_price=price_usd * self._usd_to_eur,
                stock=9999,   # API meldet keinen Lagerbestand
                part_url=part_url,
            ))

        logger.info("[MocBrickStore] %d/%d Teile gefunden", len(results), len(parts))
        return results


# Registry (ohne BrickOwlScraper – API-Key erforderlich, wird bedingt hinzugefügt)
ALL_SCRAPERS: list[type[BaseScraper]] = [
    BrickwithScraper,
    MocBrickStoreScraper,
    SNAPScraper,
    WobrickScraper,
]
