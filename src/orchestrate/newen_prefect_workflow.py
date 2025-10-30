import os
import sys
src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
project_dir = os.path.dirname(src_dir)
sys.path.append(project_dir)

from prefect import task, flow
from prefect_shell import ShellOperation

@task
def task_get_config():
    op = ShellOperation(commands=["uv run python src/extract/get_config.py"]).run()
    return op

@task
def task_extract_data():
    op = ShellOperation(commands=["uv run python src/extract/scraper_tiktokweb_post.py"]).run()
    return op

@task
def task_transform_data():
    op = ShellOperation(commands=["uv run python src/transform/transform_tiktokpost.py"]).run()
    return op

@flow
def main_flow():
    config = task_get_config.submit()
    extract = task_extract_data.submit(wait_for=[config])
    transform = task_transform_data.submit(wait_for=[extract])
 

if __name__ == "__main__":
    main_flow()