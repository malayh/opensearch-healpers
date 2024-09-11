import requests
import argparse
import logging
from requests.auth import HTTPBasicAuth
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OpenSearch:
    def __init__(self, opensearch_url, username, password):
        self.opensearch_url = opensearch_url
        self.username = username
        self.password = password
        self.auth = HTTPBasicAuth(self.username, self.password)


    def check_connection(self):
        url = f"{self.opensearch_url}/_cluster/health"
        response = requests.get(url, auth=self.auth,verify=False)
        response.raise_for_status()
        logging.info("Connection to OpenSearch successful")


    def does_data_stream_exist(self, data_stream_name: str) -> bool:
        url = f"{self.opensearch_url}/_data_stream/{data_stream_name}"
        response = requests.get(url, auth=self.auth,verify=False)
        return response.status_code == 200

    def create_data_stream(self, data_stream_name: str):
        index_template_name = f"{data_stream_name}-template"

        # create index template
        url = f"{self.opensearch_url}/_index_template/{index_template_name}"
        body = {
            "index_patterns": data_stream_name,
            "data_stream": {},
            "priority": 100
        }

        response = requests.put(url, json=body, auth=self.auth,verify=False)
        response.raise_for_status()
        logging.info(f"Created index template {index_template_name}")

        # create data stream
        url = f"{self.opensearch_url}/_data_stream/{data_stream_name}"
        response = requests.put(url, auth=self.auth,verify=False)

        if response.status_code == 400:
            error = response.json()["error"]["type"]
            if error == "resource_already_exists_exception":
                logging.info(f"Data stream {data_stream_name} already exists")
                return

        response.raise_for_status()
        logging.info(f"Created data stream {data_stream_name}")
        
    def rollover_data_stream(self, data_stream_name: str):
        if not self.does_data_stream_exist(data_stream_name):
            logging.error(f"Data stream {data_stream_name} does not exist")
            return

        url = f"{self.opensearch_url}/{data_stream_name}/_rollover"
        response = requests.post(url, auth=self.auth,verify=False)
        response.raise_for_status()
        logging.info(f"Rollover data stream {data_stream_name}")

    def delete_index(self, index_name: str):
        url = f"{self.opensearch_url}/{index_name}"
        response = requests.delete(url, auth=self.auth,verify=False)
        response.raise_for_status()
        logging.info(f"Deleted index {index_name}")

    def clean_old_data_stream_indices(self,data_stream_name,retention_period_days: int):
        if not self.does_data_stream_exist(data_stream_name):
            logging.error(f"Data stream {data_stream_name} does not exist")
            return
        
        # get all indices for the data stream
        url = f"{self.opensearch_url}/_data_stream/{data_stream_name}"
        response = requests.get(url, auth=self.auth,verify=False)
        response.raise_for_status()
        data_stream = response.json()["data_streams"][0]
        
        indices = [idx["index_name"] for idx in data_stream["indices"]]
        logging.info(f"Found indices {indices} for data stream {data_stream_name}")

        # get the creation date of each index
        indices_creation_dates = {}
        for index in indices:
            url = f"{self.opensearch_url}/{index}"
            response = requests.get(url, auth=self.auth,verify=False)
            response.raise_for_status()
            creation_date = response.json()[index]["settings"]["index"]["creation_date"]
            indices_creation_dates[index] = int(creation_date) / 1000

        for index, creation_date in indices_creation_dates.items():
            if time.time() - creation_date > retention_period_days * 24 * 60 * 60:
                logging.info(f"Deleting index {index}, age: {time.time() - creation_date} seconds")
                self.delete_index(index)

        logging.info(f"Finished cleaning data stream {data_stream_name}")
                


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenSearch Data Stream Management")
    parser.add_argument("action", choices=["create", "rollover", "clean"], help="Action to perform")
    parser.add_argument("--data-stream", required=True, help="Name of the data stream")
    parser.add_argument("--retention-period", type=int, help="Retention period in days")
    parser.add_argument("--url", required=True, help="OpenSearch URL")
    parser.add_argument("--username", required=True, help="OpenSearch username")
    parser.add_argument("--password", required=True, help="OpenSearch password")
    args = parser.parse_args()

    os = OpenSearch(args.url, args.username, args.password)
    os.check_connection()

    if args.action == "create":
        os.create_data_stream(args.data_stream)
    elif args.action == "rollover":
        os.rollover_data_stream(args.data_stream)
    elif args.action == "clean":
        if args.retention_period is None:
            parser.error("--retention-period is required for the 'clean' action")
        os.clean_old_data_stream_indices(args.data_stream, args.retention_period)

