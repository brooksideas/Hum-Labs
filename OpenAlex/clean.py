import json
import fileinput

### Clean out the Null / NaN values from the JSON line by line
file_path = "title_abstract.jsonl"  # change thefile as needed

# Open the file in-place for updating
with fileinput.FileInput(file_path, inplace=True, backup=".bak") as file:
    # Iterate through each line in the file
    for line in file:
        # Load the JSON from the line
        entry = json.loads(line.strip())

        # Check if both elements in "texts" are not null
        if all(entry["texts"]):
            # Print the line to the updated file
            print(line, end='')

print("In-place replacement completed.")
