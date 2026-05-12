"""
Multibuy-Optimierer: Verteilt LEGO-Teile optimal auf mehrere Shops.

Algorithmus (Greedy + Versandkosten-Optimierung):
  1. Für jedes Teil: finde alle Shops die es haben, sortiert nach Preis
  2. Weise jedem Teil den günstigsten Shop zu
  3. Berechne Versandkosten pro Shop (ggf. Freigrenze prüfen)
  4. Optimierungsrunde: Wenn ein Shop wegen einer kleinen Menge zusätzliche
     Versandkosten verursacht, prüfe ob das Teil woanders günstiger wäre
     (inklusive anteiliger Versandkosten)
"""

from __future__ import annotations

import logging
from collections import defaultdict

from models import OptimizeResult, OrderLine, Part, StoreOrder, StoreResult
from scrapers import BaseScraper

logger = logging.getLogger(__name__)


def optimize(
    parts: list[Part],
    store_results: dict[str, list[StoreResult]],
    scrapers: list[BaseScraper],
) -> OptimizeResult:
    """
    Berechnet den günstigsten Einkaufsplan über alle Shops.

    Args:
        parts:          Liste aller benötigten Teile (mit Mengen)
        store_results:  {shop_name → [StoreResult, ...]}
        scrapers:       Liste der Scraper-Instanzen (für Versandkosten-Info)

    Returns:
        OptimizeResult mit StoreOrders und nicht-verfügbaren Teilen
    """
    # ── 1. Verfügbarkeitsmatrix aufbauen ─────────────────────────────────────
    # availability[(part_num, color_id)] = [StoreResult, ...] sortiert nach Preis
    availability: dict[tuple[str, int], list[StoreResult]] = defaultdict(list)

    for results in store_results.values():
        for result in results:
            key = (result.part_num, result.color_id)
            availability[key].append(result)

    for key in availability:
        availability[key].sort(key=lambda r: r.unit_price)

    # ── 2. Greedy-Zuweisung: günstigstes Teil pro Shop ───────────────────────
    scraper_by_name = {s.name: s for s in scrapers}
    assignments: dict[str, list[tuple[Part, StoreResult]]] = defaultdict(list)
    unavailable: list[Part] = []

    for part in parts:
        key = (part.part_num, part.color.id)
        options = availability.get(key, [])
        # [No Color/Any Color] (9999) → auch nach color_id=0 schauen
        if not options and part.color.id == 9999:
            options = availability.get((part.part_num, 0), [])

        # Bevorzuge Shops mit ausreichend Lagerbestand
        sufficient = [r for r in options if r.stock >= part.quantity]
        partial_opts = [r for r in options if 0 < r.stock < part.quantity]

        if sufficient:
            # Sortierung: (Preis, Versand-Delta, wobrick-Priorität)
            #
            # Versand-Delta = neue Versandkosten − aktuelle Versandkosten des Shops:
            #   < 0  → dieses Teil lässt den Shop versandkostenfrei werden (super!)
            #   = 0  → keine Änderung (Shop schon frei, oder Flatrate bleibt gleich)
            #   > 0  → neuer Shop, volle Versandkosten fallen an
            #
            # Beispiele bei gleichem Stückpreis:
            #   SNAP bei 74€ + 2€-Teil → Delta = 0 − 4,95 = −4,95 → bevorzugt
            #   SNAP schon versandkostenfrei → Delta = 0 → neutral
            #   wobrick schon im Warenkorb → Delta = 0 → neutral, aber wobrick_prio=0 gewinnt
            #   wobrick neu → Delta = 3,00 → hintenangestellt
            def _sort_key(r: StoreResult) -> tuple:
                scraper = scraper_by_name.get(r.store_name)
                qty = min(part.quantity, r.stock)
                part_value = r.unit_price * qty

                if r.store_name in assignments and scraper:
                    current_total = sum(
                        res.unit_price * min(p.quantity, res.stock)
                        for p, res in assignments[r.store_name]
                    )
                    delta = (
                        scraper.calculate_shipping(current_total + part_value)
                        - scraper.calculate_shipping(current_total)
                    )
                elif scraper:
                    # Neuer Shop: volle Versandkosten für diese Bestellgröße
                    delta = scraper.calculate_shipping(part_value)
                else:
                    delta = 0.0

                wobrick_prio = 0 if r.store_name == "wobrick" else 1
                return (r.unit_price, delta, wobrick_prio)

            sufficient.sort(key=_sort_key)
            best = sufficient[0]
            assignments[best.store_name].append((part, best))
        elif partial_opts:
            # Teile aufteilen: günstigsten Teilbestand nehmen, Rest als unavailable
            best = partial_opts[0]
            assignments[best.store_name].append((part, best))
            # Fehlende Menge als nicht verfügbar markieren
            missing = part.quantity - best.stock
            import dataclasses
            remainder = dataclasses.replace(part, quantity=missing)
            unavailable.append(remainder)
            logger.debug(
                "Teilweise verfügbar: %s (%s) – %d/%d, %d fehlen",
                part.part_num, part.color.name, best.stock, part.quantity, missing,
            )
        else:
            unavailable.append(part)
            logger.debug("Nicht verfügbar: %s (%s)", part.part_num, part.color.name)

    # ── 3. Versandkosten-Optimierungsrunde ───────────────────────────────────
    # Prüfe ob Teile aus Shops mit wenig Bestellwert woanders günstiger wären
    # wenn man die Versandkosten mit einbezieht.
    assignments = _optimize_shipping(assignments, availability, scraper_by_name, parts)

    # ── 4. StoreOrder-Objekte erstellen ──────────────────────────────────────
    orders: list[StoreOrder] = []

    for store_name, part_assignments in assignments.items():
        scraper = scraper_by_name.get(store_name)
        # Wechselkurs nur für brickwith relevant
        usd_to_eur = getattr(scraper, "_usd_to_eur", 1.0)
        order = StoreOrder(
            store_name=store_name,
            store_url=scraper.base_url if scraper else "",
            original_currency=getattr(scraper, "currency", "EUR"),
            usd_to_eur=usd_to_eur,
        )

        for part, result in part_assignments:
            qty = min(part.quantity, result.stock)
            key = (part.part_num, part.color.id)
            alternatives = [
                r for r in availability.get(key, [])
                if r.store_name != store_name
            ]
            order.lines.append(OrderLine(
                part=part,
                store_name=store_name,
                unit_price=result.unit_price,
                quantity=qty,
                alternatives=alternatives,
                is_alternative=result.is_alternative,
                alt_part_num=result.alt_part_num,
                alt_color_id=result.alt_color_id,
                alt_color_name=result.alt_color_name,
                alt_color_rgb=result.alt_color_rgb,
            ))

        if scraper:
            order.shipping_cost = scraper.calculate_shipping(order.parts_total)

        # Aufträge mit Typ-Limit aufteilen (z.B. MocBrickStore: max 200 Teiletypen)
        max_types = getattr(scraper, "max_order_types", None) if scraper else None
        if max_types and len(order.lines) > max_types:
            chunks = _split_order(order, max_types, scraper)
            orders.extend(chunks)
        else:
            orders.append(order)

    orders.sort(key=lambda o: o.grand_total, reverse=True)
    return OptimizeResult(orders=orders, unavailable=unavailable)


def _split_order(
    order: StoreOrder,
    max_types: int,
    scraper,
) -> list[StoreOrder]:
    """Teilt einen Auftrag in Unter-Aufträge mit maximal max_types Zeilen auf."""
    chunks: list[StoreOrder] = []
    lines = order.lines
    for i, start in enumerate(range(0, len(lines), max_types), start=1):
        chunk_lines = lines[start : start + max_types]
        chunk = StoreOrder(
            store_name=f"{order.store_name} ({i})",
            store_url=order.store_url,
            original_currency=order.original_currency,
            usd_to_eur=order.usd_to_eur,
            lines=chunk_lines,
        )
        chunk.shipping_cost = scraper.calculate_shipping(chunk.parts_total)
        chunks.append(chunk)
    return chunks


def _optimize_shipping(
    assignments: dict[str, list[tuple[Part, StoreResult]]],
    availability: dict[tuple[str, int], list[StoreResult]],
    scraper_by_name: dict[str, BaseScraper],
    all_parts: list[Part],
) -> dict[str, list[tuple[Part, StoreResult]]]:
    """
    Optimierungsrunde: Shops mit sehr kleinem Bestellwert werden zusammengeführt,
    wenn das die Gesamtkosten (Teile + Versand) senkt.
    """
    if len(assignments) <= 1:
        return assignments

    # Berechne aktuellen Gesamtpreis
    def total_cost(assgn: dict) -> float:
        cost = 0.0
        for store_name, items in assgn.items():
            parts_sum = sum(r.unit_price * min(p.quantity, r.stock) for p, r in items)
            scraper = scraper_by_name.get(store_name)
            if scraper:
                cost += parts_sum + scraper.calculate_shipping(parts_sum)
            else:
                cost += parts_sum
        return cost

    improved = True
    max_iterations = 5

    while improved and max_iterations > 0:
        improved = False
        max_iterations -= 1

        for store_name in list(assignments.keys()):
            items = assignments[store_name]
            scraper = scraper_by_name.get(store_name)
            if not scraper:
                continue

            parts_sum = sum(r.unit_price * min(p.quantity, r.stock) for p, r in items)
            shipping = scraper.calculate_shipping(parts_sum)

            # Wenn Versandkosten > 0 und sehr wenige Teile → prüfe Alternativen
            if shipping > 0 and len(items) <= 3:
                before = total_cost(assignments)

                # Versuche alle Teile dieses Shops in andere Shops zu verschieben
                new_assignments = {k: list(v) for k, v in assignments.items() if k != store_name}

                for part, _ in items:
                    key = (part.part_num, part.color.id)
                    alternatives = [
                        r for r in availability.get(key, [])
                        if r.store_name != store_name and r.stock >= part.quantity
                    ]
                    if not alternatives:
                        # Kein Ersatz → Abbruch für diesen Shop
                        new_assignments = None
                        break
                    best_alt = alternatives[0]
                    if best_alt.store_name not in new_assignments:
                        new_assignments[best_alt.store_name] = []
                    new_assignments[best_alt.store_name].append((part, best_alt))

                if new_assignments is not None and total_cost(new_assignments) < before:
                    assignments = new_assignments
                    improved = True
                    break  # Neustart der Runde nach Änderung

    return assignments
