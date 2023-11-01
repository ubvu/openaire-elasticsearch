import os
import json
import gzip
import yaml
from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm
import elasticsearch

# Load configuration from the config.yaml file
# This configuration file should contain details about the Elasticsearch cluster, authentication credentials, and the path to the data files.
with open('config.yaml') as config_file:
    config = yaml.safe_load(config_file)
    # config now contains all the settings defined in config.yaml

try:
    # Initialize a connection to the Elasticsearch cluster with authentication
    # The connection details are retrieved from the previously loaded configuration.
    es = Elasticsearch(
        [config['elasticsearch']['cluster_url']],
        http_auth=(config['elasticsearch']['username'], config['elasticsearch']['password'])
    )
    # es is now the Elasticsearch client instance configured with the cluster URL and authentication details

    # Check if the Elasticsearch cluster is reachable
    # If not, print an error message and exit the script.
    if not es.ping():
        print("Elasticsearch cluster is not reachable! Please check your configuration.")
        exit(1)  # Exit the script if the cluster is not reachable

    # Get the path to the directory containing the compressed JSON data files
    # This path is specified in the configuration file.
    data_path = config['data_files']['path']
    # data_path is the file system path to the directory with the data files

    # Define a function to read and yield documents from the compressed JSON files
    # This function iterates through all files in the specified directory, opens the compressed files,
    # reads them line by line, and yields individual JSON objects.
    def generate_documents(data_path):
        # Iterate over each file in the specified directory
        for filename in os.listdir(data_path):
            if filename.endswith('.json.gz'):  # Check if the file is a .json.gz file
                filepath = os.path.join(data_path, filename)  # Construct the full file path
                with gzip.open(filepath, 'rt', encoding='utf-8') as open_file:
                    lines = open_file.readlines()  # Read all lines from the compressed file
                    for line in lines:
                        yield json.loads(line)  # Yield each line as a JSON object
                    return len(lines)  # Return the number of lines/documents read

    # Calculate the total number of documents to be indexed
    # This is used to set up the progress bar.
    total_docs = sum(1 for _ in generate_documents(data_path))
    print(f"Total documents to index: {total_docs}")

    # Use the helpers.bulk function to index documents in bulk
    # This involves creating a generator that yields action dictionaries for each document.
    def actions():
        # Generate the actions to be taken by Elasticsearch for each document
        for doc in generate_documents(data_path):
            # Check if a document with the same ID already exists
            if es.exists(index=config['elasticsearch']['index'], id=doc['id']):
                print(f"Document with ID {doc['id']} already exists. Skipping.")
                continue  # Skip this document if it already exists
            yield {"_op_type": "index", "_index": config['elasticsearch']['index'], "_id": doc['id'], "_source": doc}
            # Yield the action to index the document

    success, failed = helpers.bulk(es, tqdm(actions(), total=total_docs), stats_only=True)
    # Perform the bulk indexing operation, showing a progress bar
    # Initialize counters for successful and failed document indexing.

    print(f"Successfully indexed {success} documents.")
    if failed:
        print(f"Failed to index {failed} documents.")  # Print the number of failed indexing operations, if any

except elasticsearch.ElasticsearchException as e:
    # Handle Elasticsearch-specific exceptions and print an error message.
    print("An Elasticsearch error occurred:", str(e))
except Exception as e:
    # Handle any other unexpected exceptions and print an error message.
    print("An unexpected error occurred:", str(e))
