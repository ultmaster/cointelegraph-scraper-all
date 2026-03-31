# -*- coding: utf-8 -*-
"""
Created on Sun Aug 15 17:13:58 2021

@author: Oksana Bashchenko
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
import csv
import time
import json
import os
import numpy as np

#%%
output_dir = 'scraped_data'
os.makedirs(output_dir, exist_ok=True)

CSV_HEADER = ['category', 'title', 'date', 'n_views', 'n_shares', 'summary', 'content', 'tags']

#%%
def get_nice_text(soup):
    txt = ''
    for par in soup.find_all(lambda tag:tag.name=="p" and not "Related:" in tag.text):
        txt += ' ' + re.sub(" +|\n|\r|\t|\0|\x0b|\xa0",' ',par.get_text())
    return txt.strip()

#%%
def prepare_pandas(df):
    df.index = df.date
    df.drop(columns = 'date', inplace = True)
    df.index = pd.to_datetime(df.index, utc = True)
    df.sort_index(inplace = True)
    return df

#%%
def load_checkpoint(csv_path):
    """Return set of (title, date) already scraped in this CSV."""
    scraped = set()
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 3 and row[1] and row[2]:
                    scraped.add((row[1], row[2]))
    return scraped

def is_sitemap_done(csv_path, checkpoint_path):
    """A sitemap is done if its checkpoint file exists and contains 'done'."""
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path, 'r') as f:
            return f.read().strip() == 'done'
    return False

#%%
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

bad_response = []
bad_response_count = 0

#%%
def get_post_sitemap_urls(headers):
    sitemap_url = 'https://cointelegraph.com/sitemap.xml'
    sitemap_webpage = requests.get(sitemap_url, headers=headers)
    sitemap_soup = BeautifulSoup(sitemap_webpage.text, features='xml')
    sitemap_all_links = sitemap_soup.find_all('loc')
    post_urls = [link.getText() for link in sitemap_all_links if '/sitemap/post-' in link.getText()]
    return post_urls

#%%
post_sitemap_urls = get_post_sitemap_urls(headers)
print(f'Found {len(post_sitemap_urls)} post sitemaps')

total_posts = 0

for sitemap_idx, sitemap_url in enumerate(post_sitemap_urls):
    # Derive filenames from sitemap name, e.g. "post-1" -> "post-1.csv"
    sitemap_name = sitemap_url.split('/')[-1].replace('.xml', '')
    csv_path = os.path.join(output_dir, f'{sitemap_name}.csv')
    checkpoint_path = os.path.join(output_dir, f'{sitemap_name}.checkpoint')

    # Skip fully completed sitemaps
    if is_sitemap_done(csv_path, checkpoint_path):
        existing = load_checkpoint(csv_path)
        total_posts += len(existing)
        print(f'[{sitemap_idx+1}/{len(post_sitemap_urls)}] {sitemap_name} already done ({len(existing)} articles)')
        continue

    print(f'[{sitemap_idx+1}/{len(post_sitemap_urls)}] scraping {sitemap_name}...')

    # Load partial progress for this sitemap
    scraped = load_checkpoint(csv_path)
    if scraped:
        print(f'  resuming, {len(scraped)} articles already scraped')

    # Write header if CSV is new
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        with open(csv_path, 'w', encoding='utf-8') as f:
            csv.writer(f).writerow(CSV_HEADER)

    web_map = requests.get(sitemap_url, headers=headers)
    soup = BeautifulSoup(web_map.text, features='xml')
    all_links = soup.find_all('loc')
    total_urls = len(all_links)
    news_urls = [l.getText() for l in all_links if len(l.getText().split('/')) > 3 and l.getText().split('/')[3] in ("news", "markets")]
    skipped_sections = total_urls - len(news_urls)

    print(f'  found {total_urls} URLs, {len(news_urls)} are news/markets, {skipped_sections} skipped')

    posts_downloaded = 0
    posts_skipped = 0

    for article_idx, url_post in enumerate(news_urls):
        slug = url_post.split('/')[-1][:60]
        print(f'  [{article_idx+1}/{len(news_urls)}] {slug}...', end=' ', flush=True)

        page = requests.get(url_post, headers=headers)
        page.encoding = 'utf-8'

        if page.status_code != 200:
            print(f'HTTP {page.status_code}, skipping')
            bad_response.append(url_post)
            bad_response_count += 1
            continue

        sauce = BeautifulSoup(page.text, "lxml")

        # Parse LD+JSON structured data
        try:
            ld_json = json.loads(sauce.find('script', type='application/ld+json').string)
        except Exception:
            print(f'parse failed (HTTP {page.status_code}), retrying...')
            time.sleep(4)
            try:
                page = requests.get(url_post, headers=headers)
                page.encoding = 'utf-8'
                sauce = BeautifulSoup(page.text, "lxml")
                ld_json = json.loads(sauce.find('script', type='application/ld+json').string)
                print('retry OK.', end=' ')
            except Exception:
                print('retry failed, skipping')
                bad_response.append(url_post)
                bad_response_count += 1
                continue

        # Extract article data from @graph array or flat structure
        data = {}
        if '@graph' in ld_json:
            for graph_item in ld_json['@graph']:
                if graph_item.get('@type') in (['Article', 'NewsArticle'], 'NewsArticle', 'Article'):
                    data = graph_item
                    break
        else:
            data = ld_json

        art_tag = data.get('articleSection')
        date = data.get('datePublished')

        # Extract from HTML using data-testid attributes
        titleTag = sauce.find(attrs={"data-testid": "post-title"}) or sauce.find("h1")
        descTag = sauce.find(attrs={"data-testid": "post-description"})
        contentTag = sauce.find(attrs={"data-testid": "html-renderer-container"})
        article = sauce.find("article")

        title = titleTag.get_text().strip() if titleTag else data.get('headline')
        summary = descTag.get_text().strip() if descTag else None
        content = get_nice_text(contentTag) if contentTag else None

        # Skip if already scraped
        if title and date and (title, date) in scraped:
            posts_skipped += 1
            print('already scraped, skipping')
            continue

        # Log missing fields
        missing = []
        if not title: missing.append('title')
        if not date: missing.append('date')
        if not summary: missing.append('summary')
        if not content: missing.append('content')
        if missing:
            print(f'WARNING missing: {", ".join(missing)}.', end=' ')

        # Get tags only from within the article to avoid nav/footer tags
        tags_list = None
        if article:
            tag_links = article.find_all(attrs={"data-testid": "post-tag"})
            if tag_links:
                tags_list = [a.get_text().strip().lstrip('#') for a in tag_links]

        count_views = None
        count_shares = None

        with open(csv_path, 'a', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([art_tag, title, date, count_views, count_shares, summary, content, tags_list])

        if title and date:
            scraped.add((title, date))
        posts_downloaded += 1
        print(f'saved ({date})')

    total_posts += len(scraped)

    # Mark sitemap as fully done
    with open(checkpoint_path, 'w') as f:
        f.write('done')

    csv_size = os.path.getsize(csv_path) / 1024
    print(f'  done: {posts_downloaded} new, {posts_skipped} skipped, {len(scraped)} total in CSV ({csv_size:.1f} KB)')
    print(f'  cumulative: {total_posts} articles, {bad_response_count} failures')

    to_sleep = abs(np.random.normal(2, 3))
    time.sleep(to_sleep)

print(f'\n{"="*60}')
print(f'Finished. {total_posts} articles total, {bad_response_count} failures')
if bad_response:
    print(f'Failed URLs:')
    for url in bad_response:
        print(f'  {url}')
