import requests
import os
import urllib.parse


class StarfishAPIClient:

    def __init__(self, token=None, url='https://starfish.cluster.tufts.edu/api'):
        self.url = url
        self.token = token

    def get_volume_capacity(self, include_titan=False):
        res = self._send_get_request('volume')
        if include_titan:
            return res
        else:
            return [i for i in res if 'titan' not in i['vol']]

    def volume_size_query(self):
        return self._query()

    def subfolder_size_query(self, vol, path=''):
        return self.query(vol, path, query_terms={'depth': 1})

    def query(self, vol, path='', query_terms=None):
        # the path needs to urlencode any forward slashes
        path = urllib.parse.quote(path, safe='')
        query_terms = query_terms if query_terms is not None else {}
        return self._query(f'query/{vol}:{path}', query_terms=query_terms)

    def add_tag(self, vol_path, new_tag):
        """
        :return:
        """
        if not isinstance(vol_path, list):
            vol_path = [vol_path]
        if not isinstance(new_tag, list):
            new_tag = [new_tag]
        return self._send_post_request('tag/bulk',
                                       {"paths": vol_path, "tags": new_tag, "strict": False},
                                       {'Content-Type': 'application/vnd.sf.tag.bulk+json'})

    def rename_tag(self, old_tag, new_tag):
        """
        :return:
        """
        if not isinstance(old_tag, list):
            old_tag = [old_tag]
        if not isinstance(new_tag, list):
            new_tag = [new_tag]
        return self._send_post_request('tag/rename',
                                       {"tag": old_tag, "new_tag": new_tag},
                                       {'Content-Type': 'application/vnd.sf.tag.rename+json'})

    def detach_tag(self, vol_path, tag):
        if not isinstance(vol_path, list):
            vol_path = [vol_path]
        if not isinstance(tag, list):
            tag = [tag]
        return self._send_post_request('tag/detach',
                                       {"paths": vol_path, "tags": tag},
                                       {'Content-Type': 'application/vnd.sf.tag.detach+json'})

    def purge_tag(self, vol_path, tag):
        return self._send_post_request('tag/purge',
                                       {"paths": vol_path, "tags": tag},
                                       {'Content-Type': 'application/vnd.sf.tag.purge+json'})

    def _query(self, endpoint='query', query_terms=None):
        cols = ['aggrs', 'rec_aggrs', 'rec_aggrs.mtime', 'username', 'groupname', 'gid', 'tags_explicit',
                'tags_inherited', 'nlinks', 'errors', 'type_hum', 'valid_from', 'valid_to', 'cost',
                'total_capacity', 'logical_size', 'physical_size', 'physical_nlinks_size',
                'size_nlinks', 'entries_count', 'mode', 'mode_hum', 'mount_path']
        if query_terms is None:
            query_terms = {}

        query_terms.update({
                'type': 'd'
            })
        query = []
        for k, v in query_terms:
            query.append(f'{k}={v}')
        params = {'query': ' '.join(query),
                  'limit': 15000,
                  'sort_by': 'groupname',
                  'format': ' '.join(cols)}
        return self._send_get_request(endpoint, params)

    def _get_headers(self, additional_headers=None):
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        if additional_headers is not None:
            headers.update(additional_headers)
        return headers

    def _send_get_request(self, endpoint, params=None):
        r = requests.get(os.path.join(self.url, endpoint),
                         params=params if not None else {},
                         headers=self._get_headers())
        r.raise_for_status()
        return r.json()

    def _send_post_request(self, endpoint, payload, headers=None):
        r = requests.post(os.path.join(self.url, endpoint),
                          json=payload,
                          headers=self._get_headers(headers))
        r.raise_for_status()
        return r.json()






