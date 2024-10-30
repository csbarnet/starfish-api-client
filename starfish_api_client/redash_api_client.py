import requests
import os


class RedashAPIClient:
    def __init__(self, starfish_host, api_key):
        self.redash_url = f'https://{starfish_host}/redash/api/'
        self.api_key = api_key
        self.headers = {'Authorization': f'Key {api_key}'}

    def _download_file(self, url: str, local_filename: str, params: dict = None, headers: dict = None, chunk_size: int = 524_288):
        """
        downloads a file from a url, with options for setting headers, parameters, and chunk size to tune performance
        :param url: remote file location
        :param local_filename: local file location
        :param params: url query parameters
        :param headers: http headers
        :param chunk_size:
        :return:
        """

        with requests.get(url, headers=headers, params=params, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
    
    def download_query_results(self, query_id: int, local_filename: str):
        """
        get the results of a query from redash
        :param query_id: id of the
        :return:
        """
        endpoint = f'queries/{query_id}/results.csv'
        self._download_file(os.path.join(self.redash_url, endpoint), f'{query_id}.csv', headers=self.headers)