import json
import argparse
import time
from tqdm import tqdm
from sqlitedict import SqliteDict
from smart_open import open as smart_open

def load_id_title_dict(file_path):
    with smart_open(file_path, 'r') as file:
        return json.load(file)

def write_to_jsonl(file_path, id_value, works, id_title_dict):
    with smart_open(file_path, 'at') as file:
        for work in works:
            if work in id_title_dict:
                try:
                    json.dump({"texts": [id_title_dict[id_value], id_title_dict[work]]}, file)
                    file.write("\n")
                except Exception as e:
                    pass

def save_checkpoint(checkpoint_file, id_value):
    with open(checkpoint_file, 'w') as file:
        file.write(str(id_value))

def load_checkpoint(checkpoint_file):
    try:
        with open(checkpoint_file, 'r') as file:
            return int(file.read().strip())
    except FileNotFoundError:
        return 0   # this is for the first time only

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", default="work.sqlite", help="Path to the SQLite database")
    parser.add_argument("--id_title_dict_path", default="/home/jupyter/id_title_dict.json.gz", help="Path to the id_title_dict JSON file")
    parser.add_argument("--checkpoint_file", default="checkpoint.txt", help="Path to the checkpoint file")
    parser.add_argument("--batch_size", type=int, default=100000, help="Batch size for processing entries")
    args = parser.parse_args()

    # Load id_title_dict
    id_title_dict = load_id_title_dict(args.id_title_dict_path)

    # Open the SqliteDict
    db = SqliteDict(args.db_path)

    # Load checkpoint
    last_id_value = load_checkpoint(args.checkpoint_file)

    # Initialize counters for records written
    reference_count = 0
    related_count = 0

    # Get total number of entries in the database
    total_entries = len(db)

    # Start tqdm progress bar
    with tqdm(total=total_entries, desc="Processing Entries") as pbar:
        # Iterate over batches of entries
        for start_idx in range(last_id_value, total_entries, args.batch_size):
            end_idx = min(start_idx + args.batch_size, total_entries)
            batch_entries = list(db.items())[start_idx:end_idx]

            # Iterate over each batch
            for key, value in batch_entries:
                id_value = value.get('id')
                referenced_works = value.get('referenced_works', [])
                related_works = value.get('related_works', [])
                
                # Write to title_reference_dict.jsonl.gz
                write_to_jsonl("title_reference_dict.jsonl.gz", id_value, referenced_works, id_title_dict)
                reference_count += len(referenced_works)
                
                # Write to title_related_dict.jsonl.gz
                write_to_jsonl("title_related_dict.jsonl.gz", id_value, related_works, id_title_dict)
                related_count += len(related_works)
                
                # Update progress bar
                pbar.update(1)

                # Save checkpoint
                save_checkpoint(args.checkpoint_file, start_idx)
                
                # Write at the end of the batch process 
                # Use normal not gz...
                # Process a list [] , zip at the end 

                # Check if 5 minutes have passed
                if pbar.n % (5 * 60) == 0:
                    print(f"{time.strftime('%H:%M:%S')} - {pbar.n}/{total_entries} entries processed.")
                    print(f"Records written to reference file: {reference_count}")
                    print(f"Records written to related file: {related_count}")

    # Close the SqliteDict
    db.close()

if __name__ == "__main__":
    main()
