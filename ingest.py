from elasticsearch import Elasticsearch
import json
import os
import gzip
import yaml

# Load configuration from the config.yaml file
with open('config.yaml') as config_file:
    config = yaml.safe_load(config_file)

# Initialize a connection to the Elasticsearch cluster
es = Elasticsearch(config['elasticsearch']['cluster_url'])

# Initialize an empty list to store JSON documents
json_docs = []

# Get the path to the directory containing the compressed JSON data files
data_path = config['data_files']['path']

# Iterate through all files in the specified data directory
for filename in os.listdir(data_path):
    if filename.endswith('.json.gz'):
        filepath = os.path.join(data_path, filename)
        with gzip.open(filepath, 'rt', encoding='utf-8') as open_file:
            json_docs.append(json.load(open_file))

# Perform a bulk index operation to add the JSON documents to the Elasticsearch index
for doc in json_docs:
    es.index(index=config['elasticsearch']['index'], doc_type='_doc', body=doc)
