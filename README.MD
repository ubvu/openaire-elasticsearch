# OpenAIRE Elasticsearch Indexer

## Description
This tool is designed to facilitate the process of ingesting data from OpenAIRE into an Elasticsearch cluster. It reads compressed JSON data files, processes them, and indexes the documents into Elasticsearch, providing a fast and efficient search and analytics engine. The script is written in Python and utilizes the Elasticsearch Python client for interaction with the Elasticsearch cluster.

The script reads a configuration file to obtain necessary details such as the Elasticsearch cluster URL, authentication credentials, and the path to the data files. It also provides a progress bar during the data ingestion process, giving real-time feedback on the progress and helping to identify any potential issues.

![](./cover_image-openaire_elasticsearch.png)

[The code has been generated aided by GPT-4 in this conversation](https://chat.openai.com/c/eef08c3f-ec15-4240-93e2-0de5c9671657). 

## TL;DR
Quickly set up and run a Python script to ingest OpenAIRE data into Elasticsearch. Monitor progress with a real-time progress bar.

## Install
1. Clone the repository:
   ```
   git clone https://github.com/ubvu/openaire-elasticsearch/
   cd openaire-elasticsearch
   ```

2. Set up a Python virtual environment and activate it:
   ```
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

## Usage

### Configuration File
Create a `config.yaml` file in the project directory with the following structure:
```yaml
elasticsearch:
  cluster_url: 'http://localhost:9200'
  username: 'your_username'
  password: 'your_password'
  index: 'openaire_index'
data_files:
  path: '/path/to/your/data/files'
```

### Download Data Dump Files
Download the required data dump files from OpenAIRE and place them in the directory specified in your `config.yaml` file. https://graph.openaire.eu/docs/category/downloads/

### Setup ELK Stack
To visualize and analyze the data, set up an ELK (Elasticsearch, Logstash, Kibana) stack. A convenient way to do this is using Docker, and a ready-to-use Docker Compose configuration can be found at:
```
https://github.com/deviantony/docker-elk
```
Follow the instructions in that repository to get your ELK stack up and running.

### Running the Script
With everything set up, you can run the script with:
```
python bulk-ingest.py
```

## Develop

If you wish to contribute to or modify this tool, feel free to fork the repository and submit pull requests. Ensure that you follow the coding standards and write tests for new features to maintain the integrity of the tool.


## Ingest with Logstash

To ingest gzipped new line JSON files into Elasticsearch using Logstash, you can set up a Logstash pipeline with the following configuration. This configuration assumes that your JSON files are structured in a way that each line is a valid JSON object.

1. **Create a Logstash configuration file:** Save this file as `logstash-pipeline.conf` in your Logstash configuration directory.

```conf
input {
  file {
    path => "/path/to/your/data/files/*.json.gz"
    sincedb_path => "/path/to/your/sincedb_file"
    codec => "json_lines"
    mode => "read"
    file_completed_action => "log"
    file_completed_log_path => "/path/to/your/file_completed_log.log"
    start_position => "beginning"
  }
}

filter {
  gzip {
  }
}

output {
  elasticsearch {
    hosts => ["http://localhost:9200"]
    user => "your_username"
    password => "your_password"
    index => "openaire_index"
    document_type => "_doc"
  }
}
```

Replace the placeholder values in the `path`, `sincedb_path`, `file_completed_log_path`, `hosts`, `user`, `password`, and `index` fields with the appropriate values for your setup.

2. **Run Logstash with your pipeline configuration:**

```sh
bin/logstash -f /path/to/your/logstash-pipeline.conf
```

This command assumes that you are in the Logstash installation directory. Update the path to your Logstash configuration file as needed.

This pipeline configuration does the following:
- The `input` section configures Logstash to read from gzipped JSON files. The `json_lines` codec is used to handle newline-delimited JSON.
- The `filter` section includes a `gzip` filter to decompress the gzipped content.
- The `output` section configures Logstash to send the data to Elasticsearch. Make sure to provide the correct Elasticsearch cluster information and credentials.

Please note that this is a basic configuration and you might need to adjust it based on the specifics of your data and Elasticsearch setup. Additionally, ensure that the necessary plugins (like `logstash-codec-json_lines` and `logstash-filter-gzip`) are installed in your Logstash instance.