# sources/scrapers/

BeautifulSoup-based scrapers, one file per outlet, organised by country code. Each scraper is a dlt resource so it loads into the same pipeline as the API sources.

Owned by Jack (with Karina on outlet curation).

## Layout

```
scrapers/
├── _base.py        # shared pattern: fetch + parse + yield rows as a dlt resource
├── mm/             # Myanmar (Burmese)
│   ├── <outlet>.py
│   └── ...
└── kz/             # Kazakhstan (Kazakh + Russian)
    ├── <outlet>.py
    └── ...
```

## Pattern

Each outlet file looks like:

```python
import dlt
from sources.scrapers._base import Scraper

class IrrawaddyScraper(Scraper):
    name = "irrawaddy"
    country = "MM"
    base_url = "https://www.irrawaddy.com"

    def parse(self, html: str) -> list[dict]:
        # BeautifulSoup parsing
        ...

@dlt.resource(name="articles", primary_key="url", write_disposition="merge")
def irrawaddy():
    yield from IrrawaddyScraper().run()
```

## Checklist for a new outlet

1. Check `robots.txt`. Respect it.
2. Decide: RSS available? Use `sources/rss.py` instead.
3. Add to `data/config/sources.yaml`.
4. Write a `<outlet>.py` extending `Scraper`.
5. Test against a single article URL before scaling.
