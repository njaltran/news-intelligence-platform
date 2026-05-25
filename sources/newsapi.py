"""NewsAPI source (stub).

TODO (Nadi, Week 2):
    - Wire dlt rest_api source against https://newsapi.org/v2/everything
    - Auth via dlt.secrets["sources.newsapi.api_key"]
    - Paginate with `page` parameter (NewsAPI returns 100 articles per page)
    - Incremental on `publishedAt`
    - Normalise to the project schema:
        source, country_target, title, summary, url, published_at, extracted_at

See sources/gdelt.py for the resource + source pattern used here.
"""
