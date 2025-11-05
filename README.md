# Newen Rescrape Pipeline

A lightweight pipeline for extracting and transforming TikTok API data and uploading to Google Cloud Bigquery.

## Repository layout

Top-level folders and purpose:

```
project_root
├─ configs/                 # YAML configs and secret templates (GCP, proxies, cookies)
├─ data_tiktok_video/       # scraped raw/transform data by date
├─ logs/                    
├─ src/                     
│  ├─ extract/              # get configs, scraper and get cookies
│  ├─ orchestrate/          # automate scripts using task scheduler
│  ├─ transform/            # data transformation logic
│  └─ upload/               # data upload using Google Cloud Bigquery
├─ tests/                   # ad-hoc test scripts / notebooks
├─ main.py                  
├─ .python-version
├─ pyproject.toml
├─ uv.lock
└─ README.md
```

## Overview

The pipeline consists of four main stages:

1. **Extract** — Fetch TikTok data via API using configured proxies and accounts.
2. **Transform** — Clean, normalize, and enrich raw data.
3. **Upload** — Push processed data to Google Cloud BigQuery.
4. **Orchestrate** — Automate the workflow using Windows Task Scheduler.

## Prerequisites

- **Python**: 3.12 (required: >=3.12,<3.13)
- **[uv](https://docs.astral.sh/uv/getting-started/installation)**: Python package management tool.
- **Windows Task Scheduler**

## Environment setup (preferred: using `uv`)

This project supports installing dependencies from the repository metadata. If your environment uses the "uv" workflow, run the recommended sync command in the repo root:

1. **Navigate to the project directory**
    ```bash
    cd <project_directory>
    ```

2. **Install dependencies**:
    ```bash
    uv sync
    ```

## Configuration setup

### Proxy and TikTok account setup
The project reads proxy and account secrets from [configs/secrets.yaml](configs/secrets.yaml). Before running scrapers or orchestration, edit that YAML and fill in your proxy credentials — do not commit sensitive data.

Example [configs/secrets.yaml](configs/secrets.yaml) (fill with real values):

```yaml
proxy_newen:
  server: <ip:port>
  username: <proxy_username>
  password: <proxy_password>
  link_request_proxy: "https://api.provider.example/getip/<api_key>"
account:
  username: <tiktok_username>
  password: <tiktok_password>
```

### Google Cloud Platform credentials

If you plan to upload to GCS or BigQuery, point [configs/gcp.yaml](configs/gcp.yaml) to your service account file (example already references [configs/service_acount.json](configs/service_account.json)). Place the JSON credentials file at that path and keep it private.

## Execution steps

You can run each stage manually:
### 1. Extract
```bash
uv run python src/extract/get_config.py
uv run python src/extract/scraper_tiktokweb_post.py
```

### 2. Transform
```bash
uv run python src/transform/transform_tiktokpost.py
```

### 3. Upload
```bash
uv run python src/upload/upload_tiktokpost.py
```

Or, you can set it to run automatically everyday by setting up **Task Scheduler**

## Scheduling
Firstly, in the [run_automate_pipeline.bat](src/orchestrate/run_automate_pipeline.bat), modify the path of Python execution environment and the path of the [automate_pipeline.py](src/orchestrate/automate_pipeline.py).

- Open **Task Scheduler**
<img width="1931" height="1019" alt="Screenshot (209)" src="https://github.com/user-attachments/assets/5fb5fb1f-917d-4b02-a440-db8ece4728ab" />

- Start to creating a task
<img width="1931" height="1019" alt="Screenshot (209)" src="https://github.com/user-attachments/assets/bff1fc54-0ef0-4f4c-a1b9-6170ea5eefb1" />

- **General tab**: fill in the information and choose `Run whether user is logged on or not`
<img width="1930" height="1014" alt="image" src="https://github.com/user-attachments/assets/48234b8e-f435-43f8-bcef-36f815a09c6b" />

- **Triggers tab**: create a scheduler
<img width="1927" height="1005" alt="image" src="https://github.com/user-attachments/assets/eb2d294c-0628-4bf3-ae39-a81d7731a417" />
<img width="1923" height="1017" alt="Screenshot (212)" src="https://github.com/user-attachments/assets/12f6cb91-9363-4b94-906b-a4c128d284c1" />

- **Actions tab**: browse to the `run_automate_pipeline.bat` in the project
<img width="1913" height="1017" alt="image" src="https://github.com/user-attachments/assets/37902d3f-80a8-4725-9937-6747bd38c680" />
<img width="1913" height="1021" alt="image" src="https://github.com/user-attachments/assets/c923cca8-f619-4857-9eac-f6e8bf0a8a86" />

For other tabs, you can modify depend on your needs.
