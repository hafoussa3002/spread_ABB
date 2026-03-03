from __future__ import annotations

import logging
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


LOGGER = logging.getLogger(__name__)

BASE_URL = (
    "https://www.bkam.ma/Marches/Principaux-indicateurs/Marche-obligataire/"
    "Marche-des-bons-de-tresor/Marche-secondaire/Taux-de-reference-des-bons-du-tresor"
)
BLOCK_ID = "e1d6b9bbf87f86f8ba53e8518e882982"


def _to_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value


def _parse_rate(raw: object) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).strip().replace(" ", "")
    if not s:
        return None
    has_percent = "%" in s
    s = s.replace("%", "").replace(",", ".")
    try:
        val = float(s)
    except ValueError:
        return None
    if has_percent or abs(val) > 1:
        val = val / 100.0
    return val


def _parse_date(raw: object) -> Optional[date]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    for dayfirst in (True, False):
        try:
            ts = pd.to_datetime(s, dayfirst=dayfirst, errors="raise")
            return ts.date()
        except Exception:
            continue
    return None


def _normalize_col(col: str) -> str:
    return (
        col.strip()
        .lower()
        .replace("é", "e")
        .replace("è", "e")
        .replace("ê", "e")
        .replace("à", "a")
        .replace("ù", "u")
        .replace("ï", "i")
        .replace(" ", "")
        .replace("_", "")
        .replace("'", "")
        .replace("-", "")
    )


def _pick_column(columns: list[str], keys: tuple[str, ...]) -> Optional[str]:
    for c in columns:
        nc = _normalize_col(c)
        if any(k in nc for k in keys):
            return c
    return None


def _read_csv_text(text: str) -> pd.DataFrame:
    lines = text.splitlines()
    if lines:
        for i, line in enumerate(lines):
            l = line.lower()
            if ";" in line and "date" in l and "taux" in l:
                text = "\n".join(lines[i:])
                break

    for sep in (";", ",", "\t"):
        try:
            df = pd.read_csv(StringIO(text), sep=sep, dtype=str)
            if df.shape[1] >= 3:
                return df
        except Exception:
            continue
    raise ValueError("Unable to parse CSV content")


class BamCurveFetcher:
    def __init__(self, cache_dir: str | Path = "cache_bam_curves", timeout: int = 30) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Codex-BAM-Fetcher/1.0",
                "Accept": "text/csv,application/octet-stream,text/plain,*/*",
            }
        )

    def get_curve(self, curve_date: date | datetime) -> tuple[list[int], list[float]]:
        d = _to_date(curve_date)
        cache_path = self.cache_dir / f"{d.isoformat()}.csv"
        if cache_path.exists():
            text = cache_path.read_text(encoding="utf-8", errors="ignore")
            return self._parse_curve(text, d)

        text = self._download_csv_for_date(d)
        if text is None:
            raise FileNotFoundError(f"No BAM curve CSV for date {d.isoformat()}")
        cache_path.write_text(text, encoding="utf-8")
        return self._parse_curve(text, d)

    def _download_csv_for_date(self, d: date) -> Optional[str]:
        page_payloads = [
            {"date": d.strftime("%d/%m/%Y"), "block": BLOCK_ID},
            {"date": d.strftime("%Y-%m-%d"), "block": BLOCK_ID},
            {"Date": d.strftime("%d/%m/%Y"), "block": BLOCK_ID},
            {"date": d.strftime("%d/%m/%Y")},
            {},
        ]

        for payload in page_payloads:
            html = self._fetch_page(payload)
            if not html:
                continue
            links = self._extract_csv_links(html)
            for link in links:
                text = self._fetch_csv(link, params={})
                if text and self._looks_like_csv(text):
                    LOGGER.info("CSV BAM récupéré pour %s via %s params_page=%s", d, link, payload)
                    return text
        return None

    def _fetch_page(self, params: dict[str, str]) -> Optional[str]:
        try:
            resp = self.session.get(BASE_URL, params=params, timeout=self.timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            LOGGER.warning("Échec fetch page BAM params=%s: %s", params, exc)
            return None

    def _extract_csv_links(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        links: list[str] = []
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            txt = a.get_text(" ", strip=True).lower()
            if "/export/blockcsv/" in href or "csv" in txt:
                links.append(urljoin(BASE_URL, href))
        return list(dict.fromkeys(links))

    def _fetch_csv(self, url: str, params: dict[str, str]) -> Optional[str]:
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            text = resp.text
            if text and "html" not in text[:200].lower():
                return text
        except Exception:
            return None
        return None

    @staticmethod
    def _looks_like_csv(text: str) -> bool:
        if not text:
            return False
        sample = text[:1000].lower()
        if "<html" in sample:
            return False
        return (";" in text or "," in text) and "\n" in text

    def _parse_curve(self, csv_text: str, curve_date: date) -> tuple[list[int], list[float]]:
        df = _read_csv_text(csv_text)
        cols = list(df.columns)

        col_echeance = _pick_column(cols, ("echeance", "datedecheance"))
        col_valeur = _pick_column(cols, ("datevaleur", "datedelavaleur", "valeur"))
        col_taux = _pick_column(cols, ("taux", "tx"))

        if col_echeance is None and cols:
            col_echeance = cols[0]
        if col_taux is None:
            for c in cols:
                if "taux" in c.lower():
                    col_taux = c
                    break

        if col_echeance is None or col_taux is None:
            raise ValueError("CSV BAM: colonnes échéance/taux introuvables")

        rows = []
        for _, row in df.iterrows():
            d_ech = _parse_date(row.get(col_echeance))
            d_val = _parse_date(row.get(col_valeur)) if col_valeur else curve_date
            t = _parse_rate(row.get(col_taux))
            if d_ech is None or d_val is None or t is None:
                continue
            mt = (d_ech - d_val).days
            if mt <= 0:
                continue
            rows.append((mt, t))

        if not rows:
            raise ValueError("CSV BAM: aucune donnée exploitable")

        out = pd.DataFrame(rows, columns=["mt", "tx"]).dropna()
        out = out.groupby("mt", as_index=False)["tx"].mean().sort_values("mt")
        mt = out["mt"].astype(int).tolist()
        tx = out["tx"].astype(float).tolist()
        if len(mt) < 2:
            raise ValueError("CSV BAM: pas assez de points pour interpolation")
        return mt, tx

