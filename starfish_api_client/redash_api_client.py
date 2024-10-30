import requests
import os


class RedashAPIClient:
    def __init__(self, starfish_host, query_id, api_key):
        self.url = f'https://{starfish_host}/redash/api/'
        self.token = api_key
        self.query_id = query_id
    
    def query(self):
        """submit a query and return a json of the results."""
        return self._send_get_request(f'queries/{self.query_id}/results')

    def download_query_results(self, local_filename: str):
        """
        get the results of a query from redash
        :param query_id: id of the
        :return:
        """
        self._download_file(f'queries/{self.query_id}/results.csv', f'{local_filename}.csv')
    
    def _send_get_request(self, endpoint, params=None):
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Key {self.token}'
        }
        r = requests.get(os.path.join(self.url, endpoint),
                         params=params if not None else {},
                         headers=headers)
        r.raise_for_status()
        return r.json()
    
    def _download_file(self, endpoint: str, local_filename: str, chunk_size: int = 524_288):
        """
        downloads a file from a url, with options for setting headers, parameters, and chunk size to tune performance
        :param url: remote file location
        :param local_filename: local file location
        :param params: url query parameters
        :param headers: http headers
        :param chunk_size:
        :return:
        """
        headers = {
            'Authorization': f'Key {self.token}'
        }
        with requests.get(os.path.join(self.url, endpoint), headers=headers, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
