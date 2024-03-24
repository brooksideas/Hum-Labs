import json
from smart_open import open
import os
import time
 


if not os.path.exists("/home/jupyter/OA_datasets/"):
    os.mkdir("/home/jupyter/OA_datasets/")
    
pair_entities = ["domain", "field", "subfield"]  

    
for pair_entity in pair_entities:
    if os.path.exists(f"/home/jupyter/OA_datasets/title_topic_{pair_entity}_display_name.jsonl"):
        os.remove(f"/home/jupyter/OA_datasets/title_topic_{pair_entity}_display_name.jsonl")
    if os.path.exists(f"/home/jupyter/OA_datasets/title_topic_{pair_entity}_display_name.jsonl.gz"):
        os.remove(f"/home/jupyter/OA_datasets/title_topic_{pair_entity}_display_name.jsonl.gz")
        
if os.path.exists(f"/home/jupyter/OA_datasets/data_records.json"):
    os.remove(f"/home/jupyter/OA_datasets/data_records.json")
    
os.system("gcloud storage ls --recursive gs://uva-lodestone-scholarly/openalex-snapshot/updated**/part** > filenames.txt")

with open("/home/jupyter/filenames.txt") as f:
    names = f.read().splitlines()

id_title_dict = json.load(open("gs://uva-lodestone-scholarly/OA_datasets/id_title_dict.json.gz"))

data_records = {f'title_topic_{pair_entity}_display_name': 0 for pair_entity in pair_entities}

num_parsed = 0
chunks = {f'title_topic_{pair_entity}_display_name': [] for pair_entity in pair_entities}
for name in names:
    for json_line in open(name):
        result = json.loads(json_line.strip())
        if "title" in result and result["title"]:
            title = result["title"]
        else:
            continue
            
        if "topics" in result and result["topics"]:
            for topic in result["topics"]: 
                # domain_display_name = topic['domain']['display_name']
                # field_display_name = topic['field']['display_name']
                # subfield_display_name = topic['subfield']['display_name']
                domain_display_name = topic.get('domain', {}).get('display_name', '')
                field_display_name = topic.get('field', {}).get('display_name', '')
                subfield_display_name = topic.get('subfield', {}).get('display_name', '')

                
                # Append data to chunks
                chunks["title_topic_domain_display_name"].append({"texts": [title, domain_display_name]})
                chunks["title_topic_field_display_name"].append({"texts": [title, field_display_name]})
                chunks["title_topic_subfield_display_name"].append({"texts": [title, subfield_display_name]})
                 
                    
                # Update data_records count
                data_records["title_topic_domain_display_name"] += 1
                data_records["title_topic_field_display_name"] += 1
                data_records["title_topic_subfield_display_name"] += 1
        else:
            continue
        
        num_parsed += 1
        if num_parsed % 100000 == 0:
            for pair_entity in pair_entities:
                with open(f'/home/jupyter/OA_datasets/title_topic_{pair_entity}_display_name.jsonl', "a") as f:
                    for title_pair_entity_texts in chunks[f'title_topic_{pair_entity}_display_name']:
                        f.write(json.dumps(title_pair_entity_texts) + "\n")
                chunks[f'title_topic_{pair_entity}_display_name'] = []
                
            print(f'{num_parsed} works parsed')
    
print(f'Done. ({num_parsed} works parsed successfully)')

with open("/home/jupyter/OA_datasets/data_records.json", "w") as fileout:
    json.dump(data_records, fileout)
    
for pair_entity in pair_entities:
    os.system(f'gzip /home/jupyter/OA_datasets/title_topic_{pair_entity}_display_name.jsonl')
    
    time.sleep(60)

    os.system(f'gsutil cp -R /home/jupyter/OA_datasets/title_topic_{pair_entity}_display_name.jsonl.gz gs://uva-lodestone-scholarly/rnc3mm/OA_datasets/')

    # os.remove(f'/home/jupyter/OA_datasets/title_{pair_entity}.jsonl.gz')
