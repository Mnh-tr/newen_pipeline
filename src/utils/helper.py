from __future__ import annotations
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_dir)
os.chdir(project_dir)

import time
import re
import json
import shutil
import requests
import yaml
import pandas as pd
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from loguru import logger
MAX_SIZE_OF_FILE = 100 * 1024 * 1024  # 100

def delete_old_files_in_directory(directory_path, days_threshold=1):
    """Delete all files in a directory that are older than 1 day."""
    if not os.path.isdir(directory_path):
        print(f"Directory {directory_path} does not exist.")
        return
    
    # Calculate the age threshold (1 day ago)
    threshould_date = datetime.now() - timedelta(days=days_threshold)
    deleted_count = 0
    
    for root, dirs, files in os.walk(directory_path):
        for filename in files:
            file_path = os.path.join(root, filename)

            file_mod_time = os.path.getmtime(file_path)
            file_mod_date = datetime.fromtimestamp(file_mod_time)

            if file_mod_date < threshould_date:
                try:
                    os.remove(file_path)
                    print(f"Deleted {filename} (last modified: {file_mod_date})")
                    deleted_count += 1
                except Exception as e:
                    print(f"Failed to delete {filename}: {e}")

    print(f"Deleted {deleted_count} files older than {days_threshold} day(s) from {directory_path}")

def clean_category_name(cat_name):
    """
    Cleans a category name by removing special characters and replacing
    spaces, apostrophes, and forward slashes.
    
    Args:
        cat_name (str): The original category name to be cleaned
        
    Returns:
        str: The cleaned category name
    """
    import re
    
    # First replace special characters with underscores
    cleaned_name = re.sub(r'[^\w\s]', '_', cat_name)
    
    # Then remove spaces, apostrophes, and forward slashes
    cleaned_name = cleaned_name.replace(' ', '').replace("'", '').replace('/', '')
    
    return cleaned_name

def get_monday_of_week(today=None):
    if today is None:
        today = datetime.today()
    elif isinstance(today, str):
        today = datetime.strptime(today, "%Y-%m-%d")
    monday = today - timedelta(days=today.weekday())
    return monday


class FileFormat(str, Enum):
    HTML = "html"
    JSON = "json"
    YAML = "yaml"
    TXT = "txt"


def save_file(data: Any, path: str, format: FileFormat):
    try:
        if format == FileFormat.HTML:
            with open(path, "w", encoding="utf-8") as f:
                f.writelines(data)  # driver.page_source
        elif format == FileFormat.JSON:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        elif format == FileFormat.TXT:
            with open(path, "a", encoding="utf-8") as f:
                f.write(data)            
        else:
            raise ValueError(f"Unsupported file format: {format}")
    except IOError as e:
        print(f"An error occurred while saving the file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def read_file(path: str, format: FileFormat):
    try:
        if format == FileFormat.JSON:
            with open(path, "r", encoding='utf-8') as f:
                data = json.load(f)
        elif format == FileFormat.YAML:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        elif format == FileFormat.TXT:
            with open(path, "r", encoding="utf-8") as f:
                data = f.read()
        else:
            raise ValueError(f"Unsupported file format: {format}")
        return data
    except FileNotFoundError as e:
        print(f"The file was not found: {e}")
        raise
    except IOError as e:
        print(f"An error occurred while reading the file: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def regex_extract_date(folder_name):
    import re

    # folder_name = "2023-10-24-abc"
    pattern = r"\d{4}-\d{2}-\d{2}"

    match = re.search(pattern, folder_name)
    if match:
        date = match.group()
        return date
    return None


def extract_data(raw_data, fields=None):
    data = None
    if fields:
        for field in fields:
            try:
                data = raw_data[field]
                raw_data = data
            except Exception:
                # print(traceback.format_exc())
                data = None
                break
    return data


def extract_hashtags(text):
    if pd.isna(text):
        return []
    hashtags = list(
        set(re.findall(r"#(\S+)", str(text))) | set(re.findall(r"#(\w+)", str(text)))
    )
    filtered_hashtags = [tag for tag in hashtags if tag.count("#") < 2]
    return filtered_hashtags


def reset_folder(path):
    try:
        shutil.rmtree(path)
    except Exception:
        pass
    finally:
        os.makedirs(path, exist_ok=True)

def download_image(resource_url):
    if resource_url is None:
        return None
    for _ in range(5):
        try:
            resource_data = requests.get(resource_url, timeout=10).content

            if "Bad URL timestamp" in str(resource_data):
                print(f"Bad URL timestamp expired for {resource_url}")
                return None
            elif "URL signature expired" in str(resource_data):
                print(f"URL signature expired for {resource_url}")
                return None
            elif "Access Denied" in str(resource_data):
                print(f"Access denied for {resource_url}")
                return None

            return resource_data
        except requests.exceptions.RequestException as e:
            print(f"Error downloading the resource from {resource_url} at run {_}. Error: {e}. Retry...")
            # return None
            time.sleep(0.5)
    return None

def file_filter(record):
    """
    record là dict chứa log:
    - record["level"].name  -> "DEBUG", "INFO", "WARNING"...
    - record["message"]     -> nội dung log
    - record["extra"]       -> custom flag gắn thêm
    """
    return record["extra"].get("save", False)  # chỉ ghi nếu có flag save=True

def get_size_in_bytes(data):
    data_str = json.dumps(data, ensure_ascii=False)
    size_in_bytes = len(data_str.encode("utf-8"))
    return size_in_bytes


def split_data_to_batches(
    data, step: int = 2100, max_size_of_file: int = 100 * 1024 * 1024
):
    batches_data = []
    prev_i = 0
    size_in_bytes = 0
    for i in range(0, len(data), step):
        size_in_bytes += get_size_in_bytes(data[i : i + step])
        if size_in_bytes >= max_size_of_file or i + step >= len(data):
            batches_data.append(data[prev_i : i + step])
            logger.debug(
                f"size in bytes / no. records: {size_in_bytes} / {len(batches_data[-1])}"
            )
            prev_i = i + step
            size_in_bytes = 0
    return batches_data