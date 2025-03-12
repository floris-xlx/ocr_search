import sqlite3
import os
import shutil
import subprocess

def search_ocr_text(query, db_path='file_index.db'):
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT ocr_text, filename, extension FROM files')
            rows = cursor.fetchall()

            ocr_texts = [row[0] for row in rows if row[0] is not None]
            filenames = [f"{row[1]}{row[2]}" for row in rows if row[0] is not None]

            query_words = set(query.split())
            matching_indices = [
                i for i, text in enumerate(ocr_texts)
                if any(word in text for word in query_words)
            ]

            matching_filepaths = [os.path.abspath(os.path.join(r"C:\Users\floris\Desktop\mydata~1741442335680-1\chat_media", filenames[i])) for i in matching_indices]
            return matching_filepaths
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def main():
    query = input("Enter your search query: ")
    matching_files = search_ocr_text(query)
    print("Matching files:", matching_files)

    cache_dir = r'c:\Users\floris\Documents\GitHub\ocr_search\cache_search'
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    # Delete all files in the cache directory
    for filename in os.listdir(cache_dir):
        file_path = os.path.join(cache_dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

    # Copy matching files to the cache directory
    for file in matching_files:
        try:
            shutil.copy(file, cache_dir)
        except Exception as e:
            print(f"Error copying file {file}: {e}")

    if os.name == 'nt':  # Windows
        os.startfile(cache_dir)
    elif os.name == 'posix':  # macOS or Linux
        subprocess.run(['open', cache_dir] if sys.platform == 'darwin' else ['xdg-open', cache_dir])

if __name__ == "__main__":
    try:
        while True:
            main()
    except (KeyboardInterrupt, SystemExit):
        print("\nExiting the program.")
