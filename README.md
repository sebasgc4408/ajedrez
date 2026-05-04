# Extractor de datos para chatbot de ajedrez

Script en Python para unificar datos en **español** y guardarlos en **archivos locales JSONL**.

## ¿Dónde se almacena la información?

- Por defecto, el corpus se guarda en `data/chess_corpus.jsonl`.
- Puedes cambiar la ruta con `--output`, por ejemplo: `--output /tmp/corpus_ajedrez.jsonl`.

## Fuentes incluidas

- Lichess Studies API (con IDs de estudios públicos)
- Lichess Opening Explorer
- Wikipedia en español (temas de ajedrez)
- ECO openings (desde archivo local)
- Tablebase de Lichess
- Chess.com (feed RSS público de noticias/estudios)

## Uso

```bash
python3 extractor_ajedrez.py \
  --output data/chess_corpus.jsonl \
  --study-ids ABC123 DEF456 \
  --eco-file data/eco.json \
  --include-chesscom
```

## Notas importantes

- Respeta términos de uso y `robots.txt` de cada sitio.
- Para `Lichess Studies`, usa solo estudios públicos y evita scraping agresivo.
- Este extractor crea un corpus base; para un chatbot final conviene limpiar, deduplicar y chunkear.
