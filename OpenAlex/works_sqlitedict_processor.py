import concurrent.futures
import signal
import argparse
import multiprocessing
import json
import glob
import argparse
from sqlitedict import SqliteDict
from tqdm import tqdm
from smart_open import open
import time
import numpy as np
import os

# Define a function to handle timeouts
def handle_timeout(signum, frame):
    raise TimeoutError("File processing timed out")

def extract_and_save_data(json_line, db):
    try:
        data = json.loads(json_line.strip())

        # Create a new object to store only the specified column values
        new_data = {}

        # Extract and save id
        new_data['id'] = str(data.get('id', '').split('/')[-1])  # Ensure id is in string format

        # Extract and save referenced_works
        if 'referenced_works' in data:
            new_data['referenced_works'] = [url.split('/')[-1] for url in data['referenced_works']]

        # Extract and save related_works
        if 'related_works' in data:
            new_data['related_works'] = [url.split('/')[-1] for url in data['related_works']]

        # Extract and save concepts_display_name
        if 'concepts' in data:
            new_data['concepts'] = [concept.get('id', '').split('/')[-1] for concept in data['concepts']]

        # Save data to SqliteDict
        db[new_data['id']] = new_data
        
        # We have Auto-Commit to save the objects but this is a safeguard
        db.commit()

    except Exception as e:
        # pass the exceptions
        print(e)
        pass

def process_file(file_path, db_path, timeout, max_retries, delay):
    retries = 0
    while retries < max_retries:
        try:
            print("Processing file:", file_path)  # Print the current file path
            with open(file_path, 'r') as f:
                with SqliteDict(db_path, autocommit=True) as db:
                    for json_line in f:
                        extract_and_save_data(json_line, db)
            return  # Exit the loop if processing is successful
        except FileNotFoundError:
            print(f"Directory not found: {os.path.dirname(file_path)}")  # Print directory not found
            return  # Exit the loop if directory is not found
        except Exception as e:
            print(e)  # Print any other errors encountered during processing
            retries += 1
            print(f"Retrying ({retries}/{max_retries})...")
            time.sleep(delay)

    print(f"Failed to process file after {max_retries} retries:", file_path)

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file_paths",
        default= "/home/jupyter/gcs/openalex-snapshot/*/*.gz",
        help="Path pattern for files to process"
    )
    parser.add_argument(
        "--db_path",
        default="work.sqlite",
        help="Path pattern for database"
    )
    parser.add_argument(
        "--max_workers",
        default=multiprocessing.cpu_count(),
        type=int,
        help="Number of workers"
    )
    parser.add_argument(
        "--timeout",
        default=360,  # Timeout: 6 mins # Set this lower next time
        type=int,
        help="Timeout in seconds for processing each file"
    )
    parser.add_argument(
        "--max_retries",
        default=3,
        type=int,
        help="Maximum number of retries for processing each file"
    )
    parser.add_argument(
        "--retry_delay",
        default=5,
        type=int,
        help="Delay in seconds between retries"
    )
    args = parser.parse_args()

    file_paths = args.file_paths
    db_path = args.db_path
    max_workers = args.max_workers
    timeout = args.timeout
    max_retries = args.max_retries
    retry_delay = args.retry_delay

    # Register the signal handler for timeout
    signal.signal(signal.SIGALRM, handle_timeout)

    # Get list of file paths
    all_file_paths = glob.glob(file_paths)

    # Split file paths evenly among workers
    chunks = np.array_split(all_file_paths, max_workers)

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Process each chunk of file paths using map
        futures = []
        for chunk in chunks:
            for file_path in chunk:
                # Set the alarm for timeout
                signal.alarm(timeout)
                # Submit the task with timeout
                future = executor.submit(process_file, file_path, db_path, timeout, max_retries, retry_delay)
                # Wait for the task to complete or timeout
                try:
                    future.result()
                except TimeoutError as te:
                    print(te)  # Print timeout error message
                    future.cancel()  # Cancel the task
                except Exception as e:
                    print(e)  # Print any other errors
                finally:
                    signal.alarm(0)  # Cancel the alarm after task completion
