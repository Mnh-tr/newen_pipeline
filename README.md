# newen-pipeline

A small pipeline for extracting and transforming TikTok API data and uploading to Google Cloud Bigquery.

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

## Prerequisites

- **Python**: 3.12 (pyproject requires >=3.12,<3.13)
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

3. **Activate the virtual environment**:
    ```bash
    # Windows
    .venv\Scripts\activate
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

You can run each step manually, by using:
```bash
uv run python src/extract/get_config.py
```

```bash
uv run python src/extract/scraper_tiktokweb_post.py
```

```bash
uv run python src/transform/transform_tiktokpost.py
```

```bash
uv run python src/upload/upload_tiktokpost.py
```

Or, you can set it to run automatically everyday by setting up **Task Scheduler**

## Scheduling
- Open **Task Scheduler**

- Start to creating a task

- **General** tab: fill in the information and choose `Run whether user is logged on or not`

- **Triggers** tab: create a scheduler

- **Actions** tab: browse to the `run_automate_pipeline.bat` in the project

In the [run_automate_pipeline.bat](src/orchestrate/run_automate_pipeline.bat), modify the path of Python execution environment and the path of the [automate_pipeline.py](src/orchestrate/automate_pipeline.py)
