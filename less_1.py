import time
import json
from pathlib import Path
import requests


class Parse5ka:

    def __init__(self, start_url: str, result_path: Path, params: dict):
        self.start_url = start_url
        self.result_path = result_path
        self.params = params

    def _get_response(self, url, *args, **kwargs) -> requests.Response:
        while True:
            response = requests.get(url, *args, **kwargs)
            if response.status_code == 200:
                return response
            time.sleep(1)

    def run(self):
        for product in self._parse(self.start_url):
            self._save(product)

    def _parse(self, url):
        while url:
            response = self._get_response(url, params=self.params)
            data = response.json()
            url = data.get("next")
            for product in data.get("results", []):
                yield product

    def _save(self, data):
        file_path = self.result_path.joinpath(f'{data["id"]}.json')
        file_path.write_text(json.dumps(data, ensure_ascii=True))


if __name__ == "__main__":

    url_cat = 'https://5ka.ru/api/v2/categories/'
    url_cat_prod = 'https://5ka.ru/api/v2/special_offers/'
    group_cat = requests.get(url_cat).json()
    for cat in group_cat:
        cat_id = cat['parent_group_code']
        cat_name = cat['parent_group_name']
        main_path = Path(__file__).parent.joinpath("products")
        if not main_path.exists():
            main_path.mkdir()
        cat_path = main_path.joinpath(cat_name)
        if not cat_path.exists():
            cat_path.mkdir()
        groups_url = f'{url_cat}{cat_id}'
        groups = requests.get(groups_url).json()
        for group in groups:
            group_key = group['group_code']
            group_name = group['group_name']
            group_path = cat_path.joinpath(group_name)
            if not group_path.exists():
                group_path.mkdir()
            params = {'records_per_page': 20,
                      'categories': group_key}
            parser = Parse5ka(url_cat_prod, group_path, params)
            parser.run()