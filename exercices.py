import json
import elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.query import Match, MultiMatch

es = elasticsearch.Elasticsearch()
INDEX = "bd_avance"
DOC_TYPE = "restaurants"


def get_data(fname):
    with open(fname) as fhandle:
        for line in fhandle:
            filtered_data = {}
            data = json.loads(line, encoding='utf-8')
            filtered_data["id"] = data["restaurant_id"]  # Remapping the id for ES
            filtered_data["name"] = data["name"]
            filtered_data["coord"] = data["address"]["coord"]
            filtered_data["cuisine"] = data["cuisine"]
            yield filtered_data


def index_restaurants(emails):
    for e in emails:
        es.index(
            INDEX,
            DOC_TYPE,
            body=e,
            id=e['id']
        )


# 1.2
def on_74th():
    pass


# 1.3
def on_74th_10021():
    pass


# 1.4
def on_74th_10021_ordered():
    pass


# 1.5
def on_74th_10021_ordered_name():
    pass


# 1.6
def num_restaurants_on_74th():
    pass


# 2.2
def index_data():
    pass


# 2.3
def cuisines_in_manhattan():
    pass


# 2.4
def japanese_restaurants():
    pass


if __name__ == '__main__':
    pass
