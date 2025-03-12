import os
import mimetypes
import sqlite3
import hashlib
from PIL import Image
import cv2
from tqdm import tqdm
import pytesseract

# ANSI color codes
RED = '\033[91m'
GREEN = '\033[92m'
BLUE = '\033[94m'
RESET = '\033[0m'

def get_file_info(file_path):
    try:
        file_size = os.path.getsize(file_path)
        mime_type, _ = mimetypes.guess_type(file_path)
        filename, extension = os.path.splitext(os.path.basename(file_path))
        
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
            'ocr_text': ocr_text
        }
    except Exception as e:
        print(f"{RED}Error getting file info for {file_path}: {e}{RESET}")
        return None

def generate_file_id(file_info):
    hash_input = f"{file_info['size']}{file_info['filename']}{file_info['extension']}{file_info['mime_type']}"
    return hashlib.sha256(hash_input.encode()).hexdigest()

def index_folder(folder_path, db_path='file_index.db'):
    # Count total files for progress bar
    total_files = sum(len(files) for _, _, files in os.walk(folder_path))
    
    for root, _, files in os.walk(folder_path):
        print(f"{BLUE}Entering directory: {root}{RESET}")
        for file in tqdm(files, desc="Indexing files", unit="file", total=total_files):
            file_path = os.path.join(root, file)
            print(f"{BLUE}Processing file: {file_path}{RESET}")
            file_info = get_file_info(file_path)
            if file_info is None:
                print(f"{RED}Skipping file {file_path} due to previous error.{RESET}")
                continue
            file_id = generate_file_id(file_info)
            print(f"{BLUE}Generated file ID: {file_id}{RESET}")
            
            resolution_str = None
            if file_info['resolution']:
                resolution_str = f"{file_info['resolution'][0]}x{file_info['resolution'][1]}"
                print(f"{BLUE}File resolution: {resolution_str}{RESET}")
            
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS files (
                        id TEXT PRIMARY KEY,
                        filename TEXT,
                        extension TEXT,
                        size INTEGER,
                        mime_type TEXT,
                        resolution TEXT,
                        ocr_text TEXT
                    )
                ''')
                
                cursor.execute('''
                    INSERT INTO files (id, filename, extension, size, mime_type, resolution, ocr_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (file_id, file_info['filename'], file_info['extension'], file_info['size'], file_info['mime_type'], resolution_str, file_info['ocr_text']))
                conn.commit()
                print(f"{GREEN}Inserted file {file_info['filename']} into database.{RESET}")
                
                # Verify the file is in the database
                cursor.execute('SELECT * FROM files WHERE id = ?', (file_id,))
                result = cursor.fetchone()
                if result:
                    print(f"{GREEN}Verified file {file_info['filename']} is in the database.{RESET}")
                else:
                    print(f"{RED}Verification failed for file {file_info['filename']}.{RESET}")
                
            except sqlite3.IntegrityError:
                print(f"{RED}File {file_info['filename']} already exists in the database.{RESET}")
            except Exception as e:
                print(f"{RED}Error inserting file {file_info['filename']} into database: {e}{RESET}")
            finally:
                conn.close()

# Example usage
index_folder(r'C:\Users\floris\Desktop\mydata~1741442335680-1\chat_media')
