import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)
os.chdir(project_dir)

import subprocess
from loguru import logger


def run_task(script_name):
    logger.info(f"Running {script_name} ...")
    try:
        result = subprocess.run(["uv", "run", "python", script_name])
        if result.returncode != 0:
            logger.error(f"Task {script_name} failed! Stopping pipeline.")
            raise SystemExit(result.returncode)
    except KeyboardInterrupt:
        logger.warning(f"Task {script_name} interrupted by user! Stopping pipeline.")
        raise
    else:
        logger.info(f"{script_name} done.\n")

def main():
    tasks = ["src/extract/get_config.py",
             "src/extract/scraper_tiktokweb_post.py",
             "src/transform/transform_tiktokpost.py",
            #  "upload/upload_tiktokpost.py"
            ]
    for t in tasks:
        run_task(t)
    logger.info("All tasks completed successfully.")

if __name__ == "__main__":
    main()
