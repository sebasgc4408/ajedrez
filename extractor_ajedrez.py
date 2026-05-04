#!/usr/bin/env python3
"""
Extractor unificado de datos de ajedrez para chatbot.

Fuentes soportadas:
- Lichess Studies API
- Lichess Opening Explorer
- Wikipedia (ajedrez)
- ECO openings (desde archivo local)
- Tablebase (Lichess tablebase)
- Chess.com estudios/artículos (RSS público)

Salida:
- JSONL en disco local (UTF-8) para pipelines de entrenamiento/RAG.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote

import requests


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


@dataclass
class Record:
    source: str
    doc_id: str
    title: str
    url: str
    language: str
    fetched_at: str
    payload: Dict


class JSONLWriter:
    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, records: Iterable[Record]) -> int:
        count = 0
        with self.output_path.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")
                count += 1
        return count


class BaseClient:
    def __init__(self, timeout: int = 30):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.timeout = timeout

    def get_json(self, url: str) -> Dict:
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return {}

    def get_text(self, url: str) -> str:
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return ""


class LichessOpeningExplorerExtractor(BaseClient):
    BASE_URL = "https://explorer.lichess.ovh/lichess"

    def fetch(self, fen: str, moves: int = 12) -> List[Record]:
        url = f"{self.BASE_URL}?variant=standard&fen={quote(fen)}&moves={moves}&topGames=0&recentGames=0"
        data = self.get_json(url)
        if not data:
            print(f"Lichess Opening Explorer unavailable for FEN: {fen}")
            return []
        rec = Record(
            source="lichess_opening_explorer",
            doc_id=f"lichess_explorer::{fen}",
            title="Lichess Opening Explorer",
            url=url,
            language="es",
            fetched_at=_now_iso(),
            payload=data,
        )
        return [rec]


class LichessTablebaseExtractor(BaseClient):
    BASE_URL = "https://tablebase.lichess.ovh/standard"

    def fetch(self, fen: str) -> List[Record]:
        url = f"{self.BASE_URL}?fen={quote(fen)}"
        data = self.get_json(url)
        if not data:
            print(f"Lichess Tablebase unavailable for FEN: {fen}")
            return []
        rec = Record(
            source="lichess_tablebase",
            doc_id=f"lichess_tb::{fen}",
            title="Lichess Tablebase",
            url=url,
            language="es",
            fetched_at=_now_iso(),
            payload=data,
        )
        return [rec]


class WikipediaExtractor(BaseClient):
    API = "https://es.wikipedia.org/w/api.php"

    def fetch_pages(self, titles: List[str]) -> List[Record]:
        records: List[Record] = []
        for title in titles:
            url = (
                f"{self.API}?action=query&prop=extracts&explaintext=1&format=json&titles={quote(title)}"
            )
            data = self.get_json(url)
            pages = data.get("query", {}).get("pages", {})
            for page_id, page_data in pages.items():
                text = page_data.get("extract", "")
                records.append(
                    Record(
                        source="wikipedia_es_ajedrez",
                        doc_id=f"wiki::{page_id}",
                        title=page_data.get("title", title),
                        url=f"https://es.wikipedia.org/wiki/{quote(page_data.get('title', title).replace(' ', '_'))}",
                        language="es",
                        fetched_at=_now_iso(),
                        payload={"text": text},
                    )
                )
            time.sleep(0.25)
        return records


class ChessComExtractor(BaseClient):
    """Consume contenido público vía RSS de noticias/estudios publicados por Chess.com."""

    RSS_URL = "https://www.chess.com/news/rss"

    def fetch_rss(self) -> List[Record]:
        raw = self.get_text(self.RSS_URL)
        if not raw:
            print("Chess.com RSS unavailable")
            return []
        return [
            Record(
                source="chesscom_rss",
                doc_id="chesscom::news_rss",
                title="Chess.com News RSS",
                url=self.RSS_URL,
                language="es",
                fetched_at=_now_iso(),
                payload={"xml": raw},
            )
        ]


class ECOFileExtractor:
    """Carga una base ECO local en JSON/JSONL/PGN ya transformado a JSON."""

    def fetch(self, eco_file: Path) -> List[Record]:
        content = eco_file.read_text(encoding="utf-8")
        payload = {"raw": content[:2_000_000]}  # guard rail tamaño
        return [
            Record(
                source="eco_local",
                doc_id=f"eco::{eco_file.name}",
                title="ECO Openings Local",
                url=str(eco_file),
                language="es",
                fetched_at=_now_iso(),
                payload=payload,
            )
        ]


class LichessStudiesExtractor(BaseClient):
    """
    Lichess Studies requiere IDs concretos de estudio.
    Endpoint público NDJSON: /study/{id}.ndjson
    """

    def fetch(self, study_ids: List[str]) -> List[Record]:
        records: List[Record] = []
        for study_id in study_ids:
            url = f"https://lichess.org/study/{study_id}.ndjson"
            raw = self.get_text(url)
            if raw:
                records.append(
                    Record(
                        source="lichess_study",
                        doc_id=f"study::{study_id}",
                        title=f"Lichess Study {study_id}",
                        url=url,
                        language="es",
                        fetched_at=_now_iso(),
                        payload={"ndjson": raw},
                    )
                )
            else:
                print(f"Could not fetch Lichess study: {study_id}")
            time.sleep(0.4)
        return records


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extractor de fuentes de ajedrez a JSONL")
    p.add_argument("--output", default="data/chess_corpus.jsonl", help="Ruta de salida JSONL")
    p.add_argument("--fen-opening", default="rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
    p.add_argument("--fen-tablebase", default="8/8/8/8/8/8/7k/7K w - - 0 1")
    p.add_argument("--wiki-titles", nargs="*", default=["Ajedrez", "Apertura (ajedrez)", "Final (ajedrez)"])
    p.add_argument("--eco-file", default="", help="Archivo local ECO opcional")
    p.add_argument("--study-ids", nargs="*", default=[], help="IDs de estudios públicos de Lichess")
    p.add_argument("--include-chesscom", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = Path(args.output)

    records: List[Record] = []

    records.extend(LichessOpeningExplorerExtractor().fetch(args.fen_opening))
    records.extend(LichessTablebaseExtractor().fetch(args.fen_tablebase))
    records.extend(WikipediaExtractor().fetch_pages(args.wiki_titles))

    if args.study_ids:
        records.extend(LichessStudiesExtractor().fetch(args.study_ids))

    if args.eco_file:
        records.extend(ECOFileExtractor().fetch(Path(args.eco_file)))

    if args.include_chesscom:
        records.extend(ChessComExtractor().fetch_rss())

    count = JSONLWriter(out).write(records)
    print(f"OK: {count} registros guardados en {out}")


if __name__ == "__main__":
    main()
