from elasticsearch import Elasticsearch, helpers
import json
import os
import gzip
import yaml

# Load configuration from the config.yaml file
with open('config.yaml') as config_file:
    config = yaml.safe_load(config_file)

# Initialize a connection to the Elasticsearch cluster with authentication
es = Elasticsearch(
    config['elasticsearch']['cluster_url'],
    http_auth=(config['elasticsearch']['username'], config['elasticsearch']['password'])
)

# Get the path to the directory containing the compressed JSON data files
data_path = config['data_files']['path']

# Define a function to read and yield documents from the compressed JSON files
def generate_documents(data_path):
    for filename in os.listdir(data_path):
        if filename.endswith('.json.gz'):
            filepath = os.path.join(data_path, filename)
            with gzip.open(filepath, 'rt', encoding='utf-8') as open_file:
                for line in open_file:
                    yield json.loads(line)

# Use the helpers.bulk function to index documents in bulk
actions = (
    {"_op_type": "index", "_index": config['elasticsearch']['index'], "_source": doc}
    for doc in generate_documents(data_path)
)
success, failed = helpers.bulk(es, actions, stats_only=True)

print(f"Successfully indexed {success} documents.")
if failed:
    print(f"Failed to index {failed} documents.")
