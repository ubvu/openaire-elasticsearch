import os
import json
import gzip
import yaml
from elasticsearch import Elasticsearch, helpers
import elasticsearch
from tqdm import tqdm

# Load configuration from the config.yaml file.
# This configuration file should contain details about the Elasticsearch cluster, authentication credentials, and the path to the data files.
with open('config.yaml') as config_file:
    config = yaml.safe_load(config_file)

try:
    # Initialize a connection to the Elasticsearch cluster with authentication.
    # The connection details are retrieved from the previously loaded configuration.
    es = Elasticsearch(
        [config['elasticsearch']['cluster_url']],
        http_auth=(config['elasticsearch']['username'], config['elasticsearch']['password'])
    )

    # Check if the Elasticsearch cluster is reachable.
    # If not, print an error message and exit the script.
    if not es.ping():
        print("Elasticsearch cluster is not reachable! Please check your configuration.")
        exit(1)

    # Get the path to the directory containing the compressed JSON data files.
    # This path is specified in the configuration file.
    data_path = config['data_files']['path']

    # Define a function to read and yield documents from the compressed JSON files.
    # This function iterates through all files in the specified directory, opens the compressed files,
    # reads them line by line, and yields individual JSON objects.
    def generate_documents(data_path):
        for filename in os.listdir(data_path):
            if filename.endswith('.json.gz'):
                filepath = os.path.join(data_path, filename)
                with gzip.open(filepath, 'rt', encoding='utf-8') as open_file:
                    for line in open_file:
                        yield json.loads(line)

    # Calculate the total number of documents to be indexed.
    # This is used to set up the progress bar.
    total_docs = sum(1 for _ in generate_documents(data_path))
    
    # Prepare the bulk actions for Elasticsearch.
    # This involves creating a generator that yields action dictionaries for each document.
    actions = (
        {"_op_type": "index", "_index": config['elasticsearch']['index'], "_source": doc}
        for doc in generate_documents(data_path)
    )

    # Initialize counters for successful and failed document indexing.
    success, failed = 0, 0
    
    # Use the parallel_bulk helper function from the Elasticsearch library to index documents in bulk.
    # Wrap the function call in tqdm to display a progress bar.
    for ok, item in tqdm(helpers.parallel_bulk(es, actions, chunk_size=500), total=total_docs, unit="doc"):
        # Update counters based on the success or failure of each bulk operation.
        if not ok:
            failed += 1
        else:
            success += 1
    
    # Print the final results, showing the number of successfully indexed documents and any failures.
    print(f"Successfully indexed {success} documents.")
    if failed:
        print(f"Failed to index {failed} documents.")

except elasticsearch.ElasticsearchException as e:
    # Handle Elasticsearch-specific exceptions and print an error message.
    print("An Elasticsearch error occurred:", str(e))
except Exception as e:
    # Handle any other unexpected exceptions and print an error message.
    print("An unexpected error occurred:", str(e))
