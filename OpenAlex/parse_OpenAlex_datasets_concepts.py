import json
from smart_open import open
import os
import time
 


if not os.path.exists("/home/jupyter/OA_datasets/"):
    os.mkdir("/home/jupyter/OA_datasets/")
    
pair_entities = ["ancestors_related"]  

    
for pair_entity in pair_entities:
    if os.path.exists(f"/home/jupyter/OA_datasets/concept_id_{pair_entity}_display_name.jsonl"):
        os.remove(f"/home/jupyter/OA_datasets/concept_id_{pair_entity}_display_name.jsonl")
    if os.path.exists(f"/home/jupyter/OA_datasets/concept_id_{pair_entity}_display_name.jsonl.gz"):
        os.remove(f"/home/jupyter/OA_datasets/concept_id_{pair_entity}_display_name.jsonl.gz")
 
    
os.system("gcloud storage ls --recursive gs://uva-lodestone-scholarly/openalex-snapshot-concepts/openalex-snapshot/updated**/part** > concept_filenames.txt")

with open("/home/jupyter/concept_filenames.txt") as f:
    names = f.read().splitlines() 

num_parsed = 0
chunks = {f'concept_id_{pair_entity}_display_name': [] for pair_entity in pair_entities}
for name in names:
    for line in open(name):
        data = json.loads(line)
        concept_id = data["id"].split("/")[-1]  # Extract the concept ID
        
        ancestors_display_names = [ancestor["display_name"] for ancestor in data["ancestors"]]
        related_display_names = [related["display_name"] for related in data["related_concepts"]]
        
        # Prepare the output dictionary
        output_data = {
            "id": concept_id,
            "ancestors_display_names": ancestors_display_names,
            "related_display_names": related_display_names
        }
        
        # Append data to chunks
        chunks["concept_id_ancestors_related_display_name"].append(output_data)
        
        
        num_parsed += 1
        if num_parsed % 100 == 0:
            for pair_entity in pair_entities:
                with open(f'/home/jupyter/OA_datasets/concept_id_{pair_entity}_display_name.jsonl', "a") as f:
                    for concept_pair_entity in chunks[f'concept_id_{pair_entity}_display_name']:
                        f.write(json.dumps(concept_pair_entity) + "\n")
                chunks[f'concept_id_{pair_entity}_display_name'] = []
                
            print(f'{num_parsed} concepts parsed')
    
print(f'Done. ({num_parsed} concepts parsed successfully)')

    
for pair_entity in pair_entities:
    os.system(f'gzip /home/jupyter/OA_datasets/concept_id_{pair_entity}_display_name.jsonl')
