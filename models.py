from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

STATUS_PENDING = "pending"
STATUS_SEARCHING = "searching"
STATUS_PARTIAL = "partial"
STATUS_DONE = "done"
STATUS_ERROR = "error"


@dataclass
class Color:
    id: int
    name: str
    rgb: str  # hex without #, e.g. "C91A09"


@dataclass
class Part:
    part_num: str
    name: str
    color: Color
    quantity: int
    img_url: Optional[str] = None
    element_id: Optional[str] = None


@dataclass
class StoreResult:
    """Suchergebnis für ein einzelnes Teil in einem einzelnen Shop."""
    store_name: str
    part_num: str
    color_id: int
    color_name: str
    unit_price: float  # EUR pro Stück
    stock: int
    part_url: str
    is_alternative: bool = False  # True = Shop-Ersatz (andere Farbe / ohne Druck)
    # Tatsächlich geliefertes Ersatzteil (nur gesetzt wenn is_alternative=True)
    alt_part_num: str = ""    # Part-Nummer des Ersatzteils (leer = gleiche Nummer)
    alt_color_id: int = 0     # Farb-ID des Ersatzteils (0 = unbekannt)
    alt_color_name: str = ""  # Farbname des Ersatzteils
    alt_color_rgb: str = ""   # Hex-RGB des Ersatzteils (ohne #)


@dataclass
class OrderLine:
    part: Part
    store_name: str
    unit_price: float
    quantity: int
    alternatives: list["StoreResult"] = field(default_factory=list)  # andere Shops für dieses Teil
    is_alternative: bool = False  # True = Shop liefert Ersatz (andere Farbe / ohne Druck)
    # Tatsächlich geliefertes Ersatzteil (nur gesetzt wenn is_alternative=True)
    alt_part_num: str = ""
    alt_color_id: int = 0
    alt_color_name: str = ""
    alt_color_rgb: str = ""

    @property
    def line_total(self) -> float:
        return self.unit_price * self.quantity


@dataclass
class StoreOrder:
    store_name: str
    store_url: str = ""
    lines: list[OrderLine] = field(default_factory=list)
    shipping_cost: float = 0.0
    original_currency: str = "EUR"   # Originalwährung des Shops
    usd_to_eur: float = 1.0          # Verwendeter Kurs (nur relevant wenn original_currency="USD")

    @property
    def parts_total(self) -> float:
        return sum(line.line_total for line in self.lines)

    @property
    def grand_total(self) -> float:
        return self.parts_total + self.shipping_cost


@dataclass
class OptimizeResult:
    orders: list[StoreOrder]
    unavailable: list[Part]

    @property
    def total_cost(self) -> float:
        return sum(o.grand_total for o in self.orders)

    @property
    def total_parts_found(self) -> int:
        return sum(line.quantity for o in self.orders for line in o.lines)


@dataclass
class SearchSession:
    id: str
    moc_id: str
    api_key: str
    parts: list[Part]
    status: str = STATUS_PENDING
    store_progress: dict[str, str] = field(default_factory=dict)  # store -> "searching"|"done"|"error"
    store_results: dict[str, list[StoreResult]] = field(default_factory=dict)
    optimize_result: Optional[OptimizeResult] = None          # inkl. Shop-Alternativen
    optimize_result_strict: Optional[OptimizeResult] = None   # nur exakte Treffer
    result_version: int = 0                # Zähler für Live-Update der Ergebnisseite
    usd_to_eur: float = 1.0          # Wechselkurs der für diese Suche verwendet wurde
    wobrick_shipping_threshold: bool = True  # $20-Freigrenze einbeziehen
    brickowl_api_key: str = ""               # leer = BrickOwl wird nicht durchsucht
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
