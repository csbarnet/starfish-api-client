from starfish_api_client import StarfishAPIClient
import logging


LOG = logging.getLogger(__name__)

class StarfishTagger:
    def __init__(self, sf: StarfishAPIClient):
        self.sf = sf

    def add_reporting_tags(self, volume: str, path: str = '', fn_attr: str = 'fn', blacklist: list = ()) -> None:
        results = self.sf.subfolder_size_query(volume, path=path)
        filenames = StarfishTagger.get_untagged_filenames(results, fn_attr)
        filenames = StarfishTagger.filter_filenames(filenames, blacklist)
        for f in filenames:
            LOG.info(f'adding reporting tag for {f}...')
            self.sf.add_tag(f"{volume}:{f}", f"Reporting:{f}")

    @staticmethod
    def get_untagged_filenames(results: list, attribute: str) -> list:
        return [r[attribute] for r in results if 'Reporting:' not in r['tags_explicit']]

    @staticmethod
    def filter_filenames(filenames: list, blacklist: list):
        return [f for f in filenames if (f.startswith('.') or f.startswith('systemd') or f == 'mmfs' or f in blacklist)]

'''
def add_all_tags():
    sf = StarfishAPIClient(token=TOKEN)
    tagger = StarfishTagger(sf)
    tagger.add_reporting_tags('kappa')
    tagger.add_reporting_tags('kappa', path='archive/migrate', fn_attr='full_path')
    tagger.add_reporting_tags('projects')
    tagger.add_reporting_tags('other')
    # skip tagging any directories that have names that are lowercase letters. these are old home directories and nothing should be added here
    tagger.add_reporting_tags('homedir', blacklist=[chr(i) for i in range(ord("a"), ord("z") + 1)])
'''

