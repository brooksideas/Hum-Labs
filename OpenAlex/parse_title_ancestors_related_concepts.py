import json
from smart_open import open
import os
import time
 


if not os.path.exists("/home/jupyter/OA_datasets/"):
    os.mkdir("/home/jupyter/OA_datasets/")
    
pair_entities = ["ancestors", "related"]  

    
for pair_entity in pair_entities:
    if os.path.exists(f"/home/jupyter/OA_datasets/concept_{pair_entity}_display_name.jsonl"):
        os.remove(f"/home/jupyter/OA_datasets/concept_{pair_entity}_display_name.jsonl")
    if os.path.exists(f"/home/jupyter/OA_datasets/concept_{pair_entity}_display_name.jsonl.gz"):
        os.remove(f"/home/jupyter/OA_datasets/concept_{pair_entity}_display_name.jsonl.gz")
        
        
if os.path.exists(f"/home/jupyter/OA_datasets/data_records.json"):
    os.remove(f"/home/jupyter/OA_datasets/data_records.json")
 
    
os.system("gcloud storage ls --recursive gs://uva-lodestone-scholarly/openalex-snapshot/updated**/part** > filenames.txt")

with open("/home/jupyter/filenames.txt") as f:
    names = f.read().splitlines() 

id_title_dict = json.load(open("gs://uva-lodestone-scholarly/OA_datasets/id_title_dict.json.gz")) 

# Initialize an empty list to store the JSON objects
concept_data = []

# Open the JSONL file
with open("/home/jupyter/OA_datasets/concept_id_ancestors_related_display_name.jsonl.gz", "r") as file:
    # Iterate over each line in the file
    for line in file:
        # Load the JSON object from the line and append it to the list
        data = json.loads(line)
        concept_data.append(data)
print("Loaded the concepts dictionary into a list...")


data_records = {f'concept_{pair_entity}_display_name': 0 for pair_entity in pair_entities}

num_parsed = 0
chunks = {f'concept_{pair_entity}_display_name': [] for pair_entity in pair_entities}
for name in names:
    # Iterate through the work entries in the database
    for line in open(name):
        data = json.loads(line)
        work_id = data["id"].split("/")[-1]  # Extract the work ID
        concepts_list = data.get("concepts", [])
        for concept in concepts_list:  # Extract the concept ID
            concept_id = concept["id"].split("/")[-1]
            ancestors_data = []
            related_data = []
            if concept_id:
                # Loop through each dictionary in concepts_list
                for concept_dict in concept_data:
                    # Check if the current dictionary's id matches the desired id
                    if concept_dict['id'] == concept_id:
                        ancestors_display_names = concept_dict.get("ancestors_display_names", [])
                        related_display_names = concept_dict.get("related_display_names", [])

                        # Prepare data for ancestors_display_names
                        if id_title_dict.get(work_id, '')  != "":
                            ancestors_data = [{"texts": [id_title_dict.get(work_id, ''), ancestor]} for ancestor in ancestors_display_names]

                            # Prepare data for related_display_names
                            related_data = [{"texts": [id_title_dict.get(work_id, ''), related]} for related in related_display_names]
    
                        
                        break
            
            # Append data to chunks
            if len(ancestors_data):
                chunks["concept_ancestors_display_name"].append(ancestors_data[0])
                # Update data_records count only when a line is written
                data_records["concept_ancestors_display_name"] += 1
            if len(related_data):
                chunks["concept_related_display_name"].append(related_data[0]) 
                data_records["concept_related_display_name"] += 1 
             

            num_parsed += 1
            if num_parsed % 100000 == 0:
                for pair_entity in pair_entities:
                    with open(f'/home/jupyter/OA_datasets/concept_{pair_entity}_display_name.jsonl', "a") as f:
                        for concept_pair_entity in chunks[f'concept_{pair_entity}_display_name']:
                            f.write(json.dumps(concept_pair_entity) + "\n")
                    chunks[f'concept_{pair_entity}_display_name'] = []
                
            print(f'{num_parsed} concepts parsed')
    
print(f'Done. ({num_parsed} concepts parsed successfully)')

with open("/home/jupyter/OA_datasets/data_records.json", "w") as fileout:
    json.dump(data_records, fileout)

    
for pair_entity in pair_entities:
    os.system(f'gzip /home/jupyter/OA_datasets/concept_{pair_entity}_display_name.jsonl')
    
    time.sleep(60)
    
    os.system(f'gsutil cp -R /home/jupyter/OA_datasets/concept_{pair_entity}_display_name.jsonl.gz gs://uva-lodestone-scholarly/rnc3mm/OA_datasets/')
