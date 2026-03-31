# Cointelegraph full website scraper

Scrapes all news and market analysis articles from [cointelegraph.com](https://cointelegraph.com) via its sitemap.

## Output

Results are saved as one CSV per sitemap in the `scraped_data/` folder:

```
scraped_data/
  post-1.csv
  post-1.checkpoint
  post-2.csv
  post-2.checkpoint
  ...
```

Each CSV contains the following columns:
- `category` - article section (e.g. Latest News, Market Analysis)
- `title` - article headline
- `date` - publication date and time (ISO 8601 with timezone)
- `n_views` - number of views (currently unavailable via static scraping)
- `n_shares` - number of shares (currently unavailable via static scraping)
- `summary` - article summary/lead paragraph
- `content` - full article text
- `tags` - tags attributed to the article

## Usage

```bash
pip install requests beautifulsoup4 lxml pandas numpy
python scrapping_through_sitemap.py
```

The scraper supports **pause and resume**. You can stop it at any time (Ctrl+C) and rerun the same command to continue where you left off. Completed sitemaps are tracked via `.checkpoint` files, and partially-scraped sitemaps resume by skipping already-downloaded articles.

To start fresh, delete the `scraped_data/` folder.

## How it works

1. Fetches the sitemap index from `https://cointelegraph.com/sitemap.xml`
2. Iterates through each post sitemap (`post-1.xml` through `post-43.xml`)
3. Filters for `news` and `markets` articles
4. Extracts structured data from JSON-LD and HTML (`data-testid` attributes)
5. Saves results to per-sitemap CSV files with checkpoint tracking

The scraper uses the `requests` library (no Selenium/browser required) and adds random delays between sitemaps to be respectful of the server.
