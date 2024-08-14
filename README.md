# starfish-api-client

client code for interacting with Starfish API.

## Usage

```python

sc = StarfishAPIClient(token=STARFISH_TOKEN, url='https://starfish-server')
capacity_list = sc.get_volume_capacity()
volumes_list = sc.request_volumes_query()


tagger = StarfishTagger(sc)
tagger.add_reporting_tags('tag', results_list)

```
