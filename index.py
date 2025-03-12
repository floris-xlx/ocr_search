import os
import mimetypes
import sqlite3
import hashlib
import re
from PIL import Image
import cv2
from tqdm import tqdm
import pytesseract
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import sys
import time

# ANSI color codes
RED = '\033[91m'
GREEN = '\033[92m'
BLUE = '\033[94m'
RESET = '\033[0m'

# Database for performance metrics
PERF_DB_PATH = 'performance_metrics.db'

def init_perf_db():
    with sqlite3.connect(PERF_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS function_timings (
                function_name TEXT,
                execution_time_ms REAL
            )
        ''')
        conn.commit()

def log_function_timing(function_name, execution_time_ms):
    with sqlite3.connect(PERF_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO function_timings (function_name, execution_time_ms)
            VALUES (?, ?)
        ''', (function_name, execution_time_ms))
        conn.commit()

def get_slowest_function():
    with sqlite3.connect(PERF_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT function_name, AVG(execution_time_ms) as avg_time
            FROM function_timings
            GROUP BY function_name
            ORDER BY avg_time DESC
            LIMIT 1
        ''')
        result = cursor.fetchone()
        if result:
            print(f"{RED}Slowest function: {result[0]} with average time: {result[1]:.2f} ms{RESET}")

def signal_handler(sig, frame):
    print(f"{RED}\nProcess interrupted. Exiting gracefully...{RESET}")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def timed_function(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time_ms = (end_time - start_time) * 1000
        log_function_timing(func.__name__, execution_time_ms)
        return result
    return wrapper

@timed_function
def extract_date_from_filename(filename):
    match = re.match(r'^(\d{4}-\d{2}-\d{2})_', filename)
    return match.group(1) if match else None

@timed_function
def get_file_info(file_path):
    try:
        file_size = os.path.getsize(file_path)
        mime_type, _ = mimetypes.guess_type(file_path)
        filename, extension = os.path.splitext(os.path.basename(file_path))
        date_prefix = extract_date_from_filename(filename)

        resolution = None
        ocr_text = None
        if mime_type and mime_type.startswith('image'):
            with Image.open(file_path) as img:
                resolution = img.size
                ocr_text = pytesseract.image_to_string(img)
        elif mime_type and mime_type.startswith('video'):
            video = cv2.VideoCapture(file_path)
            if video.isOpened():
                width = video.get(cv2.CAP_PROP_FRAME_WIDTH)
                height = video.get(cv2.CAP_PROP_FRAME_HEIGHT)
                resolution = (int(width), int(height))
            video.release()

        return {
            'size': file_size,
            'mime_type': mime_type,
            'filename': filename,
            'extension': extension,
            'resolution': resolution,
            'ocr_text': ocr_text,
            'date': date_prefix
        }
    except Exception as e:
        print(f"{RED}Error getting file info for {file_path}: {e}{RESET}")
        return None

@timed_function
def generate_file_id(file_info):
    hash_input = f"{file_info['size']}{file_info['filename']}{file_info['extension']}{file_info['mime_type']}"
    return hashlib.sha256(hash_input.encode()).hexdigest()

@timed_function
def process_file(file_path):
    file_info = get_file_info(file_path)
    if file_info is None:
        print(f"{RED}Skipping file {file_path} due to previous error.{RESET}")
        return None

    file_id = generate_file_id(file_info)
    print(f"{BLUE}Generated file ID: {file_id}{RESET}")

    resolution_str = None
    if file_info['resolution']:
        resolution_str = f"{file_info['resolution'][0]}x{file_info['resolution'][1]}"
        print(f"{BLUE}File resolution: {resolution_str}{RESET}")

    return (file_id, file_info['filename'], file_info['extension'],
            file_info['size'], file_info['mime_type'], resolution_str,
            file_info['ocr_text'], file_info['date'])

@timed_function
def index_folder(folder_path, db_path='file_index.db'):
    # Count total files for progress bar
    total_files = sum(len(files) for _, _, files in os.walk(folder_path))
    file_list = []

    with ThreadPoolExecutor() as executor:
        futures = []
        for root, _, files in os.walk(folder_path):
            print(f"{BLUE}Entering directory: {root}{RESET}")
            for file in files:
                file_path = os.path.join(root, file)
                print(f"{BLUE}Processing file: {file_path}{RESET}")
                futures.append(executor.submit(process_file, file_path))

        for future in tqdm(as_completed(futures), desc="Indexing files", unit="file", total=total_files):
            result = future.result()
            if result:
                file_list.append(result)
                # Perform database operations every 50 files
                if len(file_list) >= 50:
                    insert_files_into_db(file_list, db_path)
                    file_list.clear()

    # Insert any remaining files
    if file_list:
        insert_files_into_db(file_list, db_path)

    get_slowest_function()

def insert_files_into_db(file_list, db_path):
    try:
        with sqlite3.connect(db_path, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    id TEXT PRIMARY KEY,
                    filename TEXT,
                    extension TEXT,
                    size INTEGER,
                    mime_type TEXT,
                    resolution TEXT,
                    ocr_text TEXT,
                    date TEXT
                )
            ''')

            cursor.executemany('''
                INSERT OR IGNORE INTO files (id, filename, extension, size, mime_type, resolution, ocr_text, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', file_list)
            conn.commit()
            print(f"{GREEN}Inserted {len(file_list)} files into database.{RESET}")
    except sqlite3.OperationalError as e:
        print(f"{RED}Error during bulk insert: {e}{RESET}")
    except Exception as e:
        print(f"{RED}Unexpected error during bulk insert: {e}{RESET}")

# Initialize performance database
init_perf_db()

# Example usage
index_folder(r'C:\Users\floris\Desktop\mydata~1741442335680-1\chat_media')
