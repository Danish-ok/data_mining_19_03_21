from pathlib import Path
import time
import requests
from urllib.parse import urljoin
import bs4
import pymongo
import datetime as dt

MONTHS = {
    "янв": 1,
    "фев": 2,
    "мар": 3,
    "апр": 4,
    "май": 5,
    "мая": 5,
    "июн": 6,
    "июл": 7,
    "авг": 8,
    "сен": 9,
    "окт": 10,
    "ноя": 11,
    "дек": 12,
}


def get_response(url):
    for _ in range(15):
        re = requests.get(url)
        if re.status_code == 200:
            return re
        time.sleep(1)
    raise ValueError('URL DIE')


def get_soup(url):
    soup = bs4.BeautifulSoup(get_response(url).text, 'lxml')
    return soup


def template():
    data_template = {
        'url': lambda a: urljoin(url, a.attrs.get('href', '/')),
        'product_name': lambda a: a.find('div', attrs={'class': 'card-sale__title'}).text,
        "promo_name": lambda a: a.find("div", attrs={"class": "card-sale__header"}).text,
        "old_price": lambda a: float(
            '.'.join(a.find("div", {"class": "label__price_old"}).text.split())),
        "new__price": lambda a: float(
            '.'.join(a.find("div", {"class": "label__price_new"}).text.split())),
        "date_from": lambda a: get_date(a.find("div", attrs={"class": "card-sale__date"}).find().text),
        "date_to": lambda a: get_date(
            a.find("div", attrs={"class": "card-sale__date"}).find().find_next('p').text),
        "image_url": lambda a: urljoin(
            url, a.find("picture").find("img").attrs.get("data-src", "/")),
    }
    return data_template


def run(start_url):
    for product in _parse(get_soup(start_url)):
        save(product)


def get_date(date_string):
    date_string = date_string.split()
    result = dt.datetime(dt.date.today().year, MONTHS[date_string[2][:3]], int(date_string[1]))
    return result


def _parse(soup):
    products_a = soup.find_all('a', attrs={'class': 'card-sale'})
    for prod_tag in products_a:
        product_data = {}
        for key, func in template().items():
            try:
                product_data[key] = func(prod_tag)
            except Exception:
                pass
        yield product_data
    pass


def save(data):
    collection = db["magnit"]
    collection.insert_one(data)


if __name__ == "__main__":
    url = "https://magnit.ru/promo/"
    mongo_url = "mongodb://localhost:27017"
    client = pymongo.MongoClient(mongo_url)
    db = client["gb_parse_19_03_21"]
    run(url)
