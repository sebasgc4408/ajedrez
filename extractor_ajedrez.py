#!/usr/bin/env python3
"""
Extractor unificado de datos de ajedrez para chatbot.

Fuentes soportadas:
- Lichess Cloud Evaluation API
- Lichess Games (públicas)
- Lichess Studies API
- Wikipedia (ajedrez)
- ECO openings (desde archivo local)
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


class LichessCloudEvalExtractor(BaseClient):
    """Fetch cloud evaluation from Lichess for chess positions."""

    def fetch(self, fen: str) -> List[Record]:
        url = f"https://lichess.org/api/cloud-eval?fen={quote(fen)}"
        data = self.get_json(url)
        if not data:
            print(f"Lichess Cloud Eval unavailable for FEN: {fen}")
            return []
        rec = Record(
            source="lichess_cloud_eval",
            doc_id=f"lichess_eval::{fen}",
            title="Lichess Cloud Evaluation",
            url=url,
            language="es",
            fetched_at=_now_iso(),
            payload=data,
        )
        return [rec]


class LichessGamesExtractor(BaseClient):
    """Fetch public games from Lichess users."""

    def fetch(self, username: str, max_games: int = 5) -> List[Record]:
        url = f"https://lichess.org/api/games/user/{username}"
        text = self.get_text(url)
        if not text:
            print(f"Lichess games unavailable for user: {username}")
            return []
        
        records: List[Record] = []
        lines = text.strip().split('\n')
        
        for i, line in enumerate(lines[:max_games]):
            try:
                records.append(
                    Record(
                        source="lichess_games",
                        doc_id=f"lichess_game::{username}::{i}",
                        title=f"Lichess Game {i+1}",
                        url=url,
                        language="es",
                        fetched_at=_now_iso(),
                        payload={"pgn": line},
                    )
                )
            except Exception as e:
                print(f"Error processing game {i}: {e}")
        
        return records


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
    p.add_argument("--fen", default="rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR")
    p.add_argument("--wiki-titles", nargs="*", default=["Ajedrez", "Apertura (ajedrez)", "Final (ajedrez)"])
    p.add_argument("--eco-file", default="", help="Archivo local ECO opcional")
    p.add_argument("--lichess-username", default="Stockfish", help="Usuario de Lichess para descargar partidas")
    p.add_argument("--study-ids", nargs="*", default=[], help="IDs de estudios públicos de Lichess")
    p.add_argument("--include-chesscom", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = Path(args.output)

    records: List[Record] = []

    # Lichess Cloud Evaluation
    records.extend(LichessCloudEvalExtractor().fetch(args.fen))
    
    # Lichess Games from user
    records.extend(LichessGamesExtractor().fetch(args.lichess_username, max_games=5))
    
    # Wikipedia
    records.extend(WikipediaExtractor().fetch_pages(args.wiki_titles))

    # Lichess Studies (if provided)
    if args.study_ids:
        records.extend(LichessStudiesExtractor().fetch(args.study_ids))

    # ECO file (if provided)
    if args.eco_file:
        records.extend(ECOFileExtractor().fetch(Path(args.eco_file)))

    # Chess.com RSS (if requested)
    if args.include_chesscom:
        records.extend(ChessComExtractor().fetch_rss())

    count = JSONLWriter(out).write(records)
    print(f"OK: {count} registros guardados en {out}")


if __name__ == "__main__":
    main()
