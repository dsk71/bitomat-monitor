#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bitomat Monitor v6 — Wrocław
============================
Monitoruje stan gotówki (PLN) w wybranych bitomatach sieci Shitcoins Club
(bitomat.com) i wysyła powiadomienia na Telegrama i/lub Discorda.

Funkcje:
  • Alerty dopasowane do realnego cyklu maszyn (serwis dowozi kasety co ~3 dni,
    wpłaty klientów NIE zwiększają salda): 🚨 zasilenie/dosypka (skok >= REFILL_JUMP),
    ⚠️ "kończy się" (spadek poniżej LOW_CASH, raz na epizod), ❌ pusto.
    Zwykłe wybieranie salda przez innych nie generuje alertów.
  • Wiele bitomatów naraz z priorytetami 🥇🥈🥉 i indywidualnymi progami kwot.
  • Poranny raport (DAILY_BRIEF_HOUR): stan maszyn, prowizje USDC/USDT,
    statystyki z ostatnich 7 dni policzone z historii CSV (liczba zasileń,
    ostatnie zasilenie, % czasu z gotówką).
  • "WARTO SPRZEDAWAĆ" — alert, gdy efektywna prowizja sprzedaży USDC spadnie do
    5% (💸) lub 2,5% (🔥), niezależnie od stanu gotówki; cichy sygnał, gdy okno się
    zamknie; werdykt + zakres z 7 dni w porannym raporcie.
  • Historia pomiarów (bitomat_history.csv) i prowizji (bitomat_fees.csv).
  • Ostrzeżenie po serii nieudanych pobrań API; ciche komunikaty serwisowe.

Prowizja liczona jest z cen podawanych przez sam bitomat w API względem
rynkowego kursu USDC/USDT w PLN (CoinGecko; awaryjnie kurs USD z NBP):
  sprzedaż (Ty sprzedajesz, bitomat skupuje):  (rynek − skup) / rynek
  kupno    (Ty kupujesz w bitomacie):          (cena − rynek) / rynek

Użycie:
  python bitomat_monitor.py               pętla ciągła (lokalnie)
  python bitomat_monitor.py --once        pojedynczy check (GitHub Actions / cron)
  python bitomat_monitor.py --test        testowe powiadomienie
  python bitomat_monitor.py --stats [DNI] statystyki z historii CSV (domyślnie 7 dni)
  python bitomat_monitor.py --list-atms Wrocław
Wymagania: pip install requests
"""

import argparse
import csv
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

# ══════════════════════════════ KONFIGURACJA ══════════════════════════════

# Które bitomaty obserwować (ID z API; sprawdzisz przez --list-atms MIASTO):
#   867  = Wrocław, Kleczkowska 5
#   1186 = Wrocław, Kuźnicza 31
#   1764 = Wrocław, Czekoladowa 7-9 (Aleja Bielany)
# KOLEJNOŚĆ = PRIORYTET (pierwszy = najbliższy/najważniejszy):
ATM_IDS = [867, 1186, 1764]      # 🥇 Kleczkowska 5, 🥈 Kuźnicza 31, 🥉 Aleja Bielany

ATM_SHORT = {867: "Kleczkowska 5", 1186: "Kuźnicza 31", 1764: "Aleja Bielany"}
PRIORITY_BADGES = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]

ATM_URLS = {                     # linki wstawiane do powiadomień (opcjonalne)
    867: "https://www.bitomat.com/bitomaty/bitomat-wroclaw-kleczkowka-5",
    1186: "https://www.bitomat.com/bitomaty/bitomat-wroclaw-kuznicza",
    1764: "https://www.bitomat.com/bitomaty/bitomat-wroclaw-aleja-bielany-czekoladowa-7",
}

API_URL = "https://shitcoins.club/atms/getAtmsData"
COINGECKO_URL = ("https://api.coingecko.com/api/v3/simple/price"
                 "?ids=usd-coin,tether&vs_currencies=pln")
NBP_URL = "https://api.nbp.pl/api/exchangerates/rates/a/usd/?format=json"

CHECK_INTERVAL_MINUTES = 60      # tylko dla trybu pętli (lokalnie); na GH rządzi cron
JITTER_SECONDS = 90

# ZASILENIE = skok salda w górę o co najmniej tyle zł (z zera lub "dosypka").
# Z 65 dni danych: każde realne zasilenie to >= 5 000 zł, wpłaty klientów nie
# zmieniają salda (trafiają do osobnego pojemnika), więc mniejsze ruchy ignorujemy.
REFILL_JUMP = 5000

# "KOŃCZY SIĘ" = saldo spadło poniżej tej kwoty (jeden alert na epizod).
# Ustaw pod kwotę, jaką typowo chcesz wypłacić.
LOW_CASH = 10000
ATM_LOW_CASH = {}                # progi indywidualne, np. {1764: 20000}

# Poranny raport: wysyłany przy PIERWSZYM pomiarze danego dnia o godzinie >= tej
# (czas polski). Jeśli GitHub pominie przebieg o 11:xx, raport pójdzie o 12:xx itd.
# None = wyłączony.
DAILY_BRIEF_HOUR = 11
DAILY_BRIEF_SILENT = True        # True = bez dźwięku (dźwięk mają tylko alerty o gotówce)
STATS_DAYS = 7                   # okno statystyk w porannym raporcie

# "WARTO SPRZEDAWAĆ": alert, gdy EFEKTYWNA prowizja sprzedaży FEE_COIN spadnie
# do/poniżej któregoś z progów (w %). Każdy próg alarmuje osobno, raz na epizod:
#   5,0 → 💸 "warto sprzedawać",  2,5 → 🔥 "bardzo tanio".
# Obserwowany zakres w tej sieci to ok. 2–8%, więc 5% to realnie "dolna połowa".
FEE_COIN = "USDC"                # śledzona moneta: "USDC" lub "USDT"
FEE_ALERT_LEVELS = [5.0, 2.5]
FEE_HYSTERESIS = 0.5             # próg "odwołuje się" dopiero, gdy prowizja wzrośnie o tyle p.p. ponad niego
FEE_QUIET_HOURS = (23, 8)        # w tych godzinach (czas PL) alert o prowizji przychodzi PO CICHU; None = zawsze głośno

# Po ilu NIEUDANYCH pobraniach API z rzędu wysłać ostrzeżenie o awarii monitora
API_FAIL_ALERT_AFTER = 5

# Najpierw sprawdzane są zmienne środowiskowe (GitHub Actions Secrets),
# a jeśli ich nie ma — wartość wpisana tu w cudzysłowach (do użytku lokalnego).
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL") or ""
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or ""
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or ""

TZ = ZoneInfo("Europe/Warsaw")
STATE_FILE = Path(__file__).with_name("bitomat_state.json")
HISTORY_FILE = Path(__file__).with_name("bitomat_history.csv")
FEES_FILE = Path(__file__).with_name("bitomat_fees.csv")
LOG_FILE = Path(__file__).with_name("bitomat_monitor.log")

# ═══════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger("bitomat")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) BitomatMonitor/6.0",
    "Accept": "application/json",
}
PL_WEEKDAYS = ["pn", "wt", "śr", "cz", "pt", "so", "nd"]


def now_local() -> datetime:
    return datetime.now(TZ)


def badge(i: int) -> str:
    return PRIORITY_BADGES[i] if i < len(PRIORITY_BADGES) else f"{i + 1}."


# ─────────────────────────────── API ───────────────────────────────────────

def fetch_all_atms() -> list[dict]:
    resp = requests.get(API_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("atms") or data.get("data") or []


def fetch_market_rates() -> dict[str, float]:
    """Rynkowe kursy USDC/USDT w PLN. Źródło: CoinGecko; awaryjnie kurs USD z NBP.
    Zwraca {} gdy oba źródła zawiodą — wtedy prowizje nie są liczone (ale reszta działa)."""
    try:
        d = requests.get(COINGECKO_URL, headers=HEADERS, timeout=15)
        d.raise_for_status()
        d = d.json()
        out = {}
        if isinstance(d.get("usd-coin", {}).get("pln"), (int, float)):
            out["USDC"] = float(d["usd-coin"]["pln"])
        if isinstance(d.get("tether", {}).get("pln"), (int, float)):
            out["USDT"] = float(d["tether"]["pln"])
        if out:
            out["_src"] = "CoinGecko"
            return out
    except Exception as e:
        log.warning("CoinGecko niedostępne (%s) — próbuję NBP.", e)
    try:
        mid = requests.get(NBP_URL, headers=HEADERS, timeout=15).json()["rates"][0]["mid"]
        return {"USDC": float(mid), "USDT": float(mid), "_src": "NBP (USD)"}
    except Exception as e:
        log.warning("NBP niedostępne (%s) — prowizje nie zostaną policzone.", e)
    return {}


def clean_place(atm: dict) -> str:
    place = (atm.get("location") or {}).get("place") or ""
    return place.replace("<br>", " ").replace("<br/>", " ").replace("\r", " ").replace("\n", " ").strip()


def atm_label(atm: dict | None, atm_id: int) -> str:
    if atm:
        loc = atm.get("location") or {}
        return f"{loc.get('city', '?')}, {clean_place(atm)}"
    return f"bitomat #{atm_id}"


def extract_state(atm: dict | None) -> dict:
    if atm is None:
        return {"online": False, "pln": None, "cash_available": None}
    pln = (atm.get("balances") or {}).get("PLN")
    return {
        "online": bool(atm.get("is_enabled", True)),
        "pln": pln if isinstance(pln, (int, float)) else None,
        "cash_available": bool(atm.get("is_cash_available")),
    }


def stable_prices(atm: dict | None) -> dict[str, dict[str, float]]:
    """Ceny USDC/USDT w bitomacie: {"USDC": {"sell": skup, "buy": sprzedaż}, ...}
    sell = ile bitomat płaci, gdy TY sprzedajesz; buy = ile płacisz, gdy kupujesz."""
    out: dict[str, dict[str, float]] = {}
    for key, val in ((atm or {}).get("prices") or {}).items():
        if not isinstance(val, (int, float)) or val <= 0:
            continue
        if key.endswith("_sell_price"):
            side, base = "sell", key[: -len("_sell_price")]
        elif key.endswith("_buy_price"):
            side, base = "buy", key[: -len("_buy_price")]
        else:
            continue
        sym = "USDT" if base.startswith("Tether") else "USDC" if base.startswith("USD Coin") else None
        if sym:
            out.setdefault(sym, {}).setdefault(side, float(val))
    return out


def fees(atm: dict | None, market: dict) -> dict[str, dict]:
    """Prowizje efektywne w % dla USDC/USDT: {"USDC": {"sell": 3.55, "buy": 3.78,
    "sell_fee": 2.7, "buy_fee": 6.1, "market": 3.65}, ...} (fee=None gdy brak rynku)."""
    out = {}
    for sym, p in stable_prices(atm).items():
        mk = market.get(sym)
        row = {"sell": p.get("sell"), "buy": p.get("buy"), "market": mk,
               "sell_fee": None, "buy_fee": None}
        if mk:
            if p.get("sell"):
                row["sell_fee"] = (mk - p["sell"]) / mk * 100
            if p.get("buy"):
                row["buy_fee"] = (p["buy"] - mk) / mk * 100
        out[sym] = row
    return out


# ─────────────────────────── Formatowanie ──────────────────────────────────

def fmt_pln(v) -> str:
    if v is None:
        return "brak danych"
    return f"{v:,.0f} zł".replace(",", " ")


def fmt_rate(v: float | None) -> str:
    return "?" if v is None else f"{v:.4f} zł".replace(".", ",")


def fmt_pct(v: float | None) -> str:
    return "?" if v is None else f"{v:.1f}%".replace(".", ",")


def fmt_days(v: float | None) -> str:
    return "?" if v is None else f"{v:.1f} d".replace(".", ",")


def fmt_when(dt: datetime) -> str:
    return f"{PL_WEEKDAYS[dt.weekday()]} {dt:%d.%m %H:%M}"


def fmt_duration(since_iso: str | None) -> str | None:
    if not since_iso:
        return None
    try:
        since = datetime.fromisoformat(since_iso)
    except ValueError:
        return None
    total_min = int((now_local() - since).total_seconds() // 60)
    if total_min < 1:
        return None
    days, rem = divmod(total_min, 1440)
    hours, minutes = divmod(rem, 60)
    if days:
        return f"{days} dni {hours} h" if hours else f"{days} dni"
    if hours:
        return f"{hours} h {minutes} min" if minutes else f"{hours} h"
    return f"{minutes} min"


def fee_lines(f: dict[str, dict], with_rates: bool = True) -> list[str]:
    """Linie z prowizjami do powiadomień."""
    lines = []
    for sym in ("USDC", "USDT"):
        row = f.get(sym)
        if not row:
            continue
        s = f"sprzedaż {fmt_pct(row['sell_fee'])}"
        b = f"kupno {fmt_pct(row['buy_fee'])}"
        if with_rates:
            s += f" (skup {fmt_rate(row['sell'])})"
            b += f" ({fmt_rate(row['buy'])})"
        lines.append(f"💸 Prowizja {sym}: {s} • {b}")
    return lines


# ─────────────────────────── Powiadomienia ─────────────────────────────────

def _redact(text: str) -> str:
    for secret in (TELEGRAM_BOT_TOKEN, DISCORD_WEBHOOK_URL):
        if secret:
            text = text.replace(secret, "***")
    return text


def send_discord(text: str, silent: bool = False) -> None:
    if not DISCORD_WEBHOOK_URL:
        return
    payload = {"content": text}
    if silent:
        payload["flags"] = 4096   # SUPPRESS_NOTIFICATIONS
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=15).raise_for_status()
        log.info("Wysłano powiadomienie na Discorda%s.", " (cicho)" if silent else "")
    except Exception as e:
        log.error("Błąd wysyłki na Discorda: %s", _redact(str(e)))


def send_telegram(text: str, silent: bool = False) -> None:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text.replace("**", ""),
                  "disable_web_page_preview": True, "disable_notification": silent},
            timeout=15,
        ).raise_for_status()
        log.info("Wysłano powiadomienie na Telegrama%s.", " (cicho)" if silent else "")
    except Exception as e:
        log.error("Błąd wysyłki na Telegrama: %s", _redact(str(e)))


def notify(text: str, silent: bool = False) -> None:
    log.info("POWIADOMIENIE%s:\n%s", " (ciche)" if silent else "", text)
    send_discord(text, silent)
    send_telegram(text, silent)


def build_atm_message(label: str, old: dict, new: dict, atm: dict | None,
                      atm_id: int, f: dict[str, dict]) -> tuple[str | None, bool]:
    """Zwraca (treść alertu lub None, low_alerted po tej zmianie)."""
    lines = []
    low_alerted = bool(old.get("low_alerted"))

    if old.get("online") and not new["online"]:
        lines.append("⚠️ Bitomat zniknął z API / jest wyłączony.")
    elif old.get("online") is False and new["online"]:
        lines.append("✅ Bitomat znów widoczny w API.")

    old_pln, new_pln = old.get("pln") or 0, new["pln"] or 0
    low = ATM_LOW_CASH.get(atm_id, LOW_CASH)

    if new_pln - old_pln >= REFILL_JUMP:
        # ── ZASILENIE (z zera albo dosypka przy saldzie > 0) ──
        if old_pln <= 0:
            lines.append(f"🚨 **ZASILENIE — GOTÓWKA DOSTĘPNA: {fmt_pln(new_pln)}!**")
            dur = fmt_duration(old.get("since"))
            if dur:
                lines.append(f"⏳ Pusto było przez: {dur}")
        else:
            lines.append(f"🚨 **DOSYPKA: +{fmt_pln(new_pln - old_pln)} → saldo {fmt_pln(new_pln)}**")
        lines.extend(fee_lines(f))
        eq = [f"{new_pln / r['sell']:,.0f} {sym}".replace(",", " ")
              for sym, r in f.items() if r.get("sell")]
        if eq:
            lines.append(f"🧮 Aby wypłacić całość, sprzedaj ≈ {' / '.join(eq)}")
        low_alerted = False
    elif old_pln > 0 and new_pln <= 0:
        lines.append(f"❌ Pusto ({fmt_pln(old_pln)} → 0 zł). Typowo zasilenie wraca w kilkanaście godzin.")
        low_alerted = False
    elif old_pln >= low > new_pln > 0 and not low_alerted:
        lines.append(f"⚠️ **Kończy się: zostało {fmt_pln(new_pln)}** (spadek poniżej {fmt_pln(low)}).")
        lines.extend(fee_lines(f, with_rates=False))
        low_alerted = True
    elif new_pln != old_pln:
        log.info("[%s] Ruch salda %s → %s — bez alertu (zwykłe wybieranie).",
                 label, fmt_pln(old_pln), fmt_pln(new_pln))

    if not lines:
        return None, low_alerted
    url = ATM_URLS.get(atm_id, "https://www.bitomat.com/mapa/bitomaty-polska")
    return f"**{label}**\n" + "\n".join(lines) + f"\n🔗 {url}", low_alerted


# ──────────────────────────── Stan i historia ──────────────────────────────

def load_state() -> dict:
    state = {}
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            log.warning("Nie udało się wczytać pliku stanu — zaczynam od zera.")
    if "atms" not in state:
        state = {"atms": {str(ATM_IDS[0]): state}} if "pln" in state else {"atms": {}}
    return state


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def append_csv(path: Path, header: list[str], row: list) -> None:
    new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        if new_file:
            w.writerow(header)
        w.writerow(row)


def append_history(ts: datetime, atm_id: int, label: str, st: dict) -> None:
    append_csv(HISTORY_FILE,
               ["timestamp", "atm_id", "miejsce", "pln", "gotowka_dostepna", "online"],
               [ts.strftime("%Y-%m-%d %H:%M:%S"), atm_id, label,
                st["pln"] if st["pln"] is not None else "",
                int(st["cash_available"]) if st["cash_available"] is not None else "",
                int(st["online"])])


def append_fees(ts: datetime, atm_id: int, f: dict[str, dict], market: dict) -> None:
    if not f:
        return
    u, t = f.get("USDC", {}), f.get("USDT", {})
    r4 = lambda v: "" if v is None else f"{v:.4f}"
    r2 = lambda v: "" if v is None else f"{v:.2f}"
    append_csv(FEES_FILE,
               ["timestamp", "atm_id", "usdc_skup", "usdc_kupno", "usdc_prow_sprzedaz_pct",
                "usdc_prow_kupno_pct", "usdt_skup", "usdt_kupno", "usdt_prow_sprzedaz_pct",
                "usdt_prow_kupno_pct", "rynek_usdc", "rynek_usdt", "zrodlo_rynku"],
               [ts.strftime("%Y-%m-%d %H:%M:%S"), atm_id,
                r4(u.get("sell")), r4(u.get("buy")), r2(u.get("sell_fee")), r2(u.get("buy_fee")),
                r4(t.get("sell")), r4(t.get("buy")), r2(t.get("sell_fee")), r2(t.get("buy_fee")),
                r4(market.get("USDC")), r4(market.get("USDT")), market.get("_src", "")])


def history_stats(days: int) -> dict[int, dict]:
    """Statystyki z bitomat_history.csv: zasilenia = skoki salda >= REFILL_JUMP
    (z zera lub dosypki). Liczba zasileń i % czasu z gotówką w oknie `days`;
    mediana odstępu i ostatnie zasilenie — z całej historii."""
    stats: dict[int, dict] = {}
    if not HISTORY_FILE.exists():
        return stats
    now = now_local()
    cutoff = now - timedelta(days=days)
    rows_by_atm: dict[int, list[tuple[datetime, float]]] = {}
    with HISTORY_FILE.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                ts = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
                atm_id = int(r["atm_id"])
            except (KeyError, ValueError):
                continue
            pln = float(r["pln"]) if r.get("pln") not in (None, "") else 0.0
            rows_by_atm.setdefault(atm_id, []).append((ts, pln))

    for atm_id, rows in rows_by_atm.items():
        rows.sort(key=lambda r: r[0])   # stabilnie po czasie (kolejność pliku przy równych)
        refills_all: list[tuple[datetime, float]] = []   # (czas, kwota zasilenia)
        cash_sec = total_sec = 0.0
        measurements = 0
        for (t0, p0), (t1, p1) in zip(rows, rows[1:]):
            if p1 - p0 >= REFILL_JUMP:
                refills_all.append((t1, p1 - p0))
            if t1 >= cutoff:
                measurements += 1
                gap = min((t1 - t0).total_seconds(), 3 * 3600)  # przerwy >3h nie liczą się
                total_sec += gap
                if p0 > 0:
                    cash_sec += gap
        gaps = [(b[0] - a[0]).total_seconds() / 86400 for a, b in zip(refills_all, refills_all[1:])]
        gaps.sort()
        stats[atm_id] = {
            "refills": [x for x in refills_all if x[0] >= cutoff],
            "refills_all": refills_all,
            "last_refill": refills_all[-1] if refills_all else None,
            "median_gap_days": gaps[len(gaps) // 2] if gaps else None,
            "cash_pct": (cash_sec / total_sec * 100) if total_sec else None,
            "measurements": measurements,
        }
    return stats


def stats_lines(stats: dict[int, dict], days: int) -> list[str]:
    lines = []
    now = now_local()
    for i, atm_id in enumerate(ATM_IDS):
        s = stats.get(atm_id)
        name = ATM_SHORT.get(atm_id, str(atm_id))
        if not s or not s["measurements"]:
            lines.append(f"{badge(i)} {name}: brak danych z ostatnich {days} dni")
            continue
        n = len(s["refills"])
        part = f"{n} zasile{'nie' if n == 1 else 'nia' if 2 <= n <= 4 else 'ń'}"
        if s["last_refill"]:
            ts, amt = s["last_refill"]
            ago = (now - ts).total_seconds() / 86400
            part += f", ost. {fmt_when(ts)} (+{fmt_pln(amt)}, {fmt_days(ago)} temu)"
        if s["median_gap_days"] is not None:
            part += f", typowo co {fmt_days(s['median_gap_days'])}"
        if s["cash_pct"] is not None:
            part += f", gotówka {fmt_pct(s['cash_pct'])} czasu"
        lines.append(f"{badge(i)} {name}: {part}")
    return lines


def fee_history_stats(days: int) -> dict | None:
    """Min/max/mediana efektywnej prowizji sprzedaży FEE_COIN z bitomat_fees.csv
    w ostatnich `days` dniach (jeden odczyt na znacznik czasu — cennik jest wspólny)."""
    if not FEES_FILE.exists():
        return None
    col = f"{FEE_COIN.lower()}_prow_sprzedaz_pct"
    cutoff = now_local() - timedelta(days=days)
    vals, seen = [], set()
    with FEES_FILE.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                ts = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
            except (KeyError, ValueError):
                continue
            if ts < cutoff or r["timestamp"] in seen or r.get(col) in (None, ""):
                continue
            seen.add(r["timestamp"])
            try:
                vals.append(float(r[col]))
            except ValueError:
                continue
    if not vals:
        return None
    vals.sort()
    return {"min": vals[0], "max": vals[-1], "median": vals[len(vals) // 2], "n": len(vals)}


def fee_range_line(days: int) -> str | None:
    fs = fee_history_stats(days)
    if not fs:
        return None
    return (f"📊 Prowizja {FEE_COIN} ost. {days} dni: {fmt_pct(fs['min'])}–{fmt_pct(fs['max'])}"
            f" (mediana {fmt_pct(fs['median'])})")


# ─────────────────────────────── Logika ────────────────────────────────────

def current_fees(watched: dict, market: dict) -> dict[str, dict]:
    """Cennik jest wspólny dla całej sieci — bierze pierwszy obserwowany bitomat z cenami."""
    for atm_id in ATM_IDS:
        f = fees(watched.get(atm_id), market)
        if f:
            return f
    return {}


def fee_verdict(fee: float | None) -> str:
    """Jednozdaniowa ocena prowizji do porannego raportu."""
    if fee is None:
        return "❔ Prowizja nieznana (brak kursu rynkowego)"
    low, high = min(FEE_ALERT_LEVELS), max(FEE_ALERT_LEVELS)
    if fee <= low:
        return f"🔥 BARDZO TANIO — warto sprzedawać ({fmt_pct(fee)} ≤ {fmt_pct(low)})"
    if fee <= high:
        return f"✅ Warto sprzedawać ({fmt_pct(fee)} ≤ {fmt_pct(high)})"
    return f"⛔ Drogo — lepiej poczekać ({fmt_pct(fee)} > {fmt_pct(high)})"


def is_quiet_hour(now: datetime) -> bool:
    if not FEE_QUIET_HOURS:
        return False
    start, end = FEE_QUIET_HOURS
    h = now.hour
    return (h >= start or h < end) if start > end else (start <= h < end)


def daily_brief(state: dict, watched: dict, market: dict) -> str | None:
    if DAILY_BRIEF_HOUR is None:
        return None
    now = now_local()
    today = now.strftime("%Y-%m-%d")
    if now.hour < DAILY_BRIEF_HOUR or state.get("last_brief") == today:
        return None
    state["last_brief"] = today

    lines = [f"☀️ Raport bitomatów — {fmt_when(now)}", ""]
    for i, atm_id in enumerate(ATM_IDS):
        atm = watched.get(atm_id)
        st = extract_state(atm)
        dur = fmt_duration((state["atms"].get(str(atm_id)) or {}).get("since"))
        extra = f" (bez zmian od {dur})" if dur else ""
        lines.append(f"{badge(i)} {ATM_SHORT.get(atm_id, atm_label(atm, atm_id))}: "
                     f"{fmt_pln(st['pln'])}{extra}")

    # prowizje z pierwszego dostępnego bitomatu (cennik jest wspólny dla sieci)
    for atm_id in ATM_IDS:
        f = fees(watched.get(atm_id), market)
        if f:
            lines.append("")
            lines.extend(fee_lines(f))
            if market:
                lines.append(f"📈 Rynek: USDC {fmt_rate(market.get('USDC'))} • "
                             f"USDT {fmt_rate(market.get('USDT'))} ({market.get('_src')})")
            lines.append(fee_verdict((f.get(FEE_COIN) or {}).get("sell_fee")))
            rng = fee_range_line(STATS_DAYS)
            if rng:
                lines.append(rng)
            break

    stats = history_stats(STATS_DAYS)
    if stats:
        lines.append("")
        lines.append(f"📊 Ostatnie {STATS_DAYS} dni:")
        lines.extend(stats_lines(stats, STATS_DAYS))
    return "\n".join(lines)


def fee_message(state: dict, watched: dict, market: dict) -> tuple[str | None, str | None]:
    """Zwraca (alert głośny, komunikat cichy).
    Głośny: prowizja sprzedaży FEE_COIN spadła do/poniżej progu z FEE_ALERT_LEVELS,
    którego jeszcze nie zgłoszono w tym epizodzie. Cichy: prowizja wróciła powyżej
    najwyższego progu (+ histereza) — okno "warto sprzedawać" się zamknęło."""
    f = current_fees(watched, market)
    row = f.get(FEE_COIN) or {}
    fee = row.get("sell_fee")
    if fee is None:
        return None, None

    active = {float(x) for x in state.get("fee_levels_active", [])}
    newly, closed = [], []
    for lvl in FEE_ALERT_LEVELS:
        if fee <= lvl and lvl not in active:
            active.add(lvl)
            newly.append(lvl)
        elif fee > lvl + FEE_HYSTERESIS and lvl in active:
            active.discard(lvl)
            closed.append(lvl)
    state["fee_levels_active"] = sorted(active)
    state["fee_last"] = round(fee, 2)

    if newly:
        lvl = min(newly)
        head = "🔥 **BARDZO NISKA PROWIZJA**" if lvl < max(FEE_ALERT_LEVELS) else "💸 **WARTO SPRZEDAWAĆ**"
        lines = [f"{head} — prowizja sprzedaży {FEE_COIN}: {fmt_pct(fee)} (próg {fmt_pct(lvl)})",
                 f"📉 Skup {FEE_COIN}: {fmt_rate(row.get('sell'))} • rynek {fmt_rate(row.get('market'))}"
                 f" ({market.get('_src', '?')})"]
        other = "USDT" if FEE_COIN == "USDC" else "USDC"
        o = f.get(other) or {}
        if o.get("sell_fee") is not None:
            lines.append(f"   {other}: {fmt_pct(o['sell_fee'])} (skup {fmt_rate(o.get('sell'))})")
        rng = fee_range_line(STATS_DAYS)
        if rng:
            lines.append(rng)
        cash = " • ".join(
            f"{badge(i)} {ATM_SHORT.get(a, a)}: {fmt_pln(extract_state(watched.get(a))['pln'])}"
            for i, a in enumerate(ATM_IDS))
        lines.append(f"💵 Gotówka: {cash}")
        if all((extract_state(watched.get(a))["pln"] or 0) <= 0 for a in ATM_IDS):
            lines.append("   (maszyny puste — wypłata po najbliższym zasileniu)")
        lines.append(f"🔗 {ATM_URLS.get(ATM_IDS[0], 'https://www.bitomat.com')}")
        return "\n".join(lines), None

    if closed and max(FEE_ALERT_LEVELS) in closed:
        return None, (f"ℹ️ Prowizja sprzedaży {FEE_COIN} wzrosła do {fmt_pct(fee)} — "
                      f"okno ≤{fmt_pct(max(FEE_ALERT_LEVELS))} zamknięte.")
    if closed:
        log.info("Prowizja %s wzrosła do %s — poziom %s odwołany.", FEE_COIN, fmt_pct(fee), closed)
    return None, None


def check_once() -> None:
    state = load_state()
    try:
        all_atms = fetch_all_atms()
    except Exception as e:
        log.error("Błąd pobierania API: %s", e)
        fails = int(state.get("api_fail_count", 0)) + 1
        state["api_fail_count"] = fails
        if fails == API_FAIL_ALERT_AFTER:
            notify(f"⚠️ Monitor od {fails} sprawdzeń z rzędu nie może pobrać danych z API "
                   f"bitomatów.\nOstatni błąd: {e}\nPróbuję dalej — dam znać, gdy wróci.")
        save_state(state)
        return
    if int(state.get("api_fail_count", 0)) >= API_FAIL_ALERT_AFTER:
        notify("✅ Monitor znów pobiera dane z API — awaria zakończona.", silent=True)
    state["api_fail_count"] = 0

    market = fetch_market_rates()
    watched = {a["id"]: a for a in all_atms if a.get("id") in ATM_IDS}
    ts = now_local()
    messages = []

    for i, atm_id in enumerate(ATM_IDS):
        atm = watched.get(atm_id)
        label = atm_label(atm, atm_id)
        new = extract_state(atm)
        f = fees(atm, market)
        append_history(ts, atm_id, label, new)
        append_fees(ts, atm_id, f, market)
        u = f.get("USDC", {})
        log.info("[%s] PLN=%s, cash=%s, online=%s | USDC skup %s (prow. %s), kupno %s (prow. %s)",
                 label, fmt_pln(new["pln"]), new["cash_available"], new["online"],
                 fmt_rate(u.get("sell")), fmt_pct(u.get("sell_fee")),
                 fmt_rate(u.get("buy")), fmt_pct(u.get("buy_fee")))

        old = state["atms"].get(str(atm_id))
        if not old:
            log.info("[%s] Pierwszy pomiar — zapisuję stan początkowy bez alertu.", label)
            state["atms"][str(atm_id)] = {**new, "since": ts.isoformat()}
            continue

        msg, low_alerted = build_atm_message(f"{badge(i)} {label}", old, new, atm, atm_id, f)
        if msg:
            messages.append(msg)

        since = old.get("since") or ts.isoformat()
        if old.get("pln") != new["pln"] or old.get("online") != new["online"]:
            since = ts.isoformat()
        state["atms"][str(atm_id)] = {**new, "since": since, "low_alerted": low_alerted}

    if messages:
        summary = " • ".join(
            f"{badge(i)} {ATM_SHORT.get(a, a)}: {fmt_pln(extract_state(watched.get(a))['pln'])}"
            for i, a in enumerate(ATM_IDS)
        )
        messages.append(f"📊 Stan wszystkich: {summary}")

    fee_loud, fee_quiet = fee_message(state, watched, market)
    brief = daily_brief(state, watched, market)

    if messages:
        notify("\n\n".join(messages))
    else:
        log.info("Brak zmian.")
    if fee_loud:
        notify(fee_loud, silent=is_quiet_hour(ts))   # w nocy po cichu (FEE_QUIET_HOURS)
    if fee_quiet:
        notify(fee_quiet, silent=True)
    if brief:
        notify(brief, silent=DAILY_BRIEF_SILENT)
    save_state(state)


def list_atms(city: str) -> None:
    atms = fetch_all_atms()
    hits = [a for a in atms if city.lower() in ((a.get("location") or {}).get("city") or "").lower()]
    if not hits:
        print(f"Nie znaleziono bitomatów dla: {city}")
        return
    print(f"{'ID':>6}  {'PLN':>10}  {'gotówka':>8}  miejsce")
    for a in sorted(hits, key=lambda x: x.get("id", 0)):
        st = extract_state(a)
        print(f"{a.get('id'):>6}  {fmt_pln(st['pln']):>10}  "
              f"{'TAK' if st['cash_available'] else 'NIE':>8}  {atm_label(a, a.get('id'))}")


def print_stats(days: int) -> None:
    stats = history_stats(days)
    if not stats:
        print("Brak pliku historii lub brak danych.")
        return
    print(f"Statystyki z ostatnich {days} dni:")
    for line in stats_lines(stats, days):
        print(" ", line)
    rng = fee_range_line(days)
    if rng:
        print(" ", rng)
    for i, atm_id in enumerate(ATM_IDS):
        s = stats.get(atm_id)
        if s and s["refills_all"]:
            print(f"\n  {badge(i)} {ATM_SHORT.get(atm_id, atm_id)} — zasilenia (cała historia):")
            for ts, amt in s["refills_all"]:
                print(f"     {fmt_when(ts)}  +{fmt_pln(amt):>12}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor gotówki w bitomatach")
    parser.add_argument("--once", action="store_true", help="pojedynczy check (GitHub Actions / cron)")
    parser.add_argument("--test", action="store_true", help="wyślij testowe powiadomienie i zakończ")
    parser.add_argument("--stats", nargs="?", const=STATS_DAYS, type=int, metavar="DNI",
                        help="statystyki z historii CSV (domyślnie 7 dni)")
    parser.add_argument("--list-atms", metavar="MIASTO", help="wypisz bitomaty w mieście wraz z ID")
    args = parser.parse_args()

    if args.test:
        notify("🧪 Test powiadomień — monitor bitomatów.\nJeśli to widzisz, wszystko działa. 🎉")
        return
    if args.stats is not None:
        print_stats(args.stats)
        return
    if args.list_atms:
        list_atms(args.list_atms)
        return
    if args.once:
        check_once()
        return

    log.info("Start monitora — sprawdzam co %s min. Ctrl+C aby zakończyć.", CHECK_INTERVAL_MINUTES)
    while True:
        check_once()
        time.sleep(max(60, CHECK_INTERVAL_MINUTES * 60 + random.randint(-JITTER_SECONDS, JITTER_SECONDS)))


if __name__ == "__main__":
    main()
