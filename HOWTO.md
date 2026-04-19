# How to Use Structured Data Parser

A comprehensive guide to using the parsers for various data sources.

---

## Table of Contents

1. [Airtable](#airtable)
2. [Notion](#notion)
3. [Excel / Google Sheets](#excel--google-sheets)
4. [GitHub API](#github-api)
5. [Wikipedia & Wikidata](#wikipedia--wikidata)
6. [PDF Tables](#pdf-tables)
7. [HTML Tables](#html-tables)
8. [Markdown](#markdown)
9. [WordPress](#wordpress)

---

## Airtable

### Installation

```bash
pip install requests
```

### Authentication

Set your Airtable Personal Access Token:

```bash
export AIRTABLE_TOKEN=pat.xxxxxxxx
```

### Basic Usage

```python
from parse_airtable import fetch_all, to_json, to_csv

# Fetch all records from a table
records = fetch_all("appXXXXXXXX", "Tasks")

# Convert to JSON
print(to_json(records))

# Convert to CSV
print(to_csv(records))
```

### CLI Usage

```bash
# List all bases
python -m parse_airtable bases

# Fetch records as CSV
python -m parse_airtable records appXXXXXXXX "Tasks" --format csv --out output.csv

# Filter with formulas
python -m parse_airtable records appXXXXXXXX "Tasks" --formula "AND({Status}='Open')"
```

---

## Notion

### Installation

```bash
pip install notion-client
```

### Authentication

Create an integration at [notion.so/my-integrations](https://www.notion.so/my-integrations) and share your database with it.

```bash
export NOTION_TOKEN=secret_xxxx
```

### Basic Usage

```python
from parse_notion import query_database, fetch_page, fetch_blocks
from parse_notion import to_json, to_csv, blocks_to_markdown

# Query a database
pages = query_database("DATABASE_ID")

# Fetch a page's properties
page = fetch_page("PAGE_ID")

# Fetch page content (blocks)
blocks = fetch_blocks("PAGE_ID")

# Convert to different formats
print(to_json(pages))
print(to_csv(pages))
print(blocks_to_markdown(blocks))
```

### CLI Usage

```bash
# Query database as JSON
python -m parse_notion query DATABASE_ID --format json --out data.json

# Get database schema
python -m parse_notion schema DATABASE_ID

# Fetch page as Markdown
python -m parse_notion blocks PAGE_ID --format markdown
```

---

## Excel / Google Sheets

### Installation

```bash
pip install openpyxl  # For .xlsx, .xlsm
pip install xlrd    # For legacy .xls
pip install ezodf   # For .ods
```

### Basic Usage

```python
from parse_excel import parse, parse_all_sheets, to_json, to_csv

# Parse a specific sheet
records = parse("data.xlsx", sheet="Revenue")

# Parse all sheets
all_data = parse_all_sheets("data.xlsx")

# Convert formats
print(to_json(records))
print(to_csv(records))
```

### CLI Usage

```bash
# List sheets in a workbook
python -m parse_excel data.xlsx --list-sheets

# Parse first sheet as CSV
python -m parse_excel data.xlsx --format csv --out out.csv

# Skip metadata rows
python -m parse_excel data.xlsx --skip 2 --sheet "Data"
```

---

## GitHub API

### Installation

```bash
pip install requests
```

### Authentication

```bash
export GITHUB_TOKEN=ghp_xxxx
```

### Basic Usage

```python
from parse_github_api import fetch_issues, fetch_pulls, fetch_releases
from parse_github_api import fetch_repo, to_json, to_csv

# Fetch issues
issues = fetch_issues("owner", "repo", state="open")

# Fetch releases
releases = fetch_releases("owner", "repo")

# Get repository info
repo = fetch_repo("owner", "repo")

print(to_json(releases))
```

---

## Wikipedia & Wikidata

### Installation

```bash
pip install mwparserfromhell sparqlwrapper
```

### Wikipedia Usage

```python
from parse_wikipedia import parse_article, to_json

# Extract article content
records = parse_article("Python_(programming_language)")
print(to_json(records))
```

### Wikidata Usage

```python
from parse_wikidata import query_sparql, fetch_entity

# Query entities
results = query_sparql("""
SELECT ?item ?itemLabel WHERE {
  ?item wdt:P31 wd:Q5.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
LIMIT 10
""")

# Fetch specific entity
entity = fetch_entity("Q5")  # human
```

---

## PDF Tables

### Installation

```bash
pip install camelot
```

### Basic Usage

```python
from parse_pdf_tables import parse_pdf, to_json, to_csv

# Extract tables from PDF
tables = parse_pdf("report.pdf")
print(to_json(tables))
```

---

## HTML Tables

### Installation

```bash
pip install beautifulsoup4 lxml
```

### Basic Usage

```python
from parse_html_tables import parse_url, parse_file, to_json

# Parse tables from URL
tables = parse_url("https://example.com/data")
print(to_json(tables))

# Parse from local HTML
tables = parse_file("page.html")
```

### CLI Usage

```bash
python -m parse_html_tables https://example.com/tables --format csv
```

---

## Markdown

### Installation

```bash
pip install tabulate
```

### Basic Usage

```python
from parse_markdown_tables import parse_file, to_json

# Parse markdown tables
tables = parse_file("README.md")
print(to_json(tables))
```

---

## WordPress

### Installation

```bash
pip install requests
```

### Basic Usage

```python
from parse_wordpress import fetch_posts, fetch_pages, to_json

# Fetch posts
posts = fetch_posts("https://example.com/wp-json/wp/v2")

# Fetch pages
pages = fetch_pages("https://example.com/wp-json/wp/v2")

print(to_json(posts))
```

### CLI Usage

```bash
python -m parse_wordpress https://example.com/wp-json/wp/v2/posts --format json
```

---

## Common Patterns

### Converting to Pandas DataFrame

```python
from parse_airtable import fetch_all
import pandas as pd

records = fetch_all("appXXX", "Tasks")
df = pd.DataFrame(records)

# Now use pandas
df.describe()
df.to_csv("output.csv")
```

### Scheduled Fetching with Cron

```bash
# Add to crontab
0 */6 * * * python -m parse_airtable records appXXX Tasks --format json --out /data/tasks.json
```

### Error Handling

```python
from parse_airtable import fetch_all
import logging

try:
    records = fetch_all("appXXX", "Tasks")
except Exception as e:
    logging.error(f"Failed to fetch: {e}")
    raise
```

---

*For more examples, see the test files in the repository.*