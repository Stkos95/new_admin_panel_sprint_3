import requests
import json
import elasticsearch
from elasticsearch import helpers
state = """{
    "mappings": {
        "properties": {
            "text_field": {"type": "keyword"},
            "number": {"type": "long"}
        }
    }
}"""



# with open('schema.json', 'r') as file:
#     schema = json.load(file)
# r = requests.put(url='http://127.0.0.1:9200/movies3', headers={'Content-Type': 'application/json'}, data=state)
# print(json.loads(r.text))

{
}

dd = '''{
    "text_field": "my pretty text",
    "number": 15
}'''
# r = requests.post('http://127.0.0.1:9200/movies3/_search/', headers={'Content-Type': 'application/json'})
# r = requests.post('http://127.0.0.1:9200/movies3/_doc/1213', headers={'Content-Type': 'application/json'}, data = dd)
# print()
# print(json.loads(r.text))

from pprint import pprint
class ElasticSearchLoader:
    
    def __init__(self, index_name, path_scheme: str = None):
        self.index = index_name
        self.client = elasticsearch.Elasticsearch("http://localhost:9200")

    def _load_schema(self, path_file: str) -> str:
        with open(path_file, 'r') as file:
            schema = file.read()
        return schema

    def create_index(self, schema: str):
        schema = self._load_schema(schema)
        url = f'http://127.0.0.1:9200/{self.index}/'
        r = self._send_request('put', url, headers={'Content-Type': 'application/json'}, data=schema)
        if r.get('status') == 400:
            #logger 
            print(f'Индекс {self.index} уже создан.')
        elif r.get('acknowledged'):
            print(f'Индекс {self.index} успешно создан!')
        return r

    def insert_data(self, data: dict):
        url = f'http://127.0.0.1:9200/{self.index_name}/_doc/'
        return self._send_request('post', url, headers={'Content-Type': 'application/json'}, data=data)
    
    def update_data(self, indx: str, data: dict):
        url = f'http://127.0.0.1:9200/{self.index_name}/_doc/{indx}/'
        return self._send_request('post', url, headers={'Content-Type': 'application/json'}, data=data) 



    def batch_insert_data(self, data: dict):
        # prepared_data = self.create_statement_bach_insert(data)
        return helpers.bulk(self.client, data, index=self.index)


    # def batch_insert_data(self, data: dict):
    #     url = f'http://127.0.0.1:9200/_bulk?filter_path=items.*.error'

    #     # prepared_data = self.create_statement_bach_insert(data)
    #     # # pprint(prepared_data)
    #     # prepared_data = [json.dumps(el) for el in prepared_data]
    #     # prepared_data = ''.join(prepared_data)
    #     # prepared_data += '\n\n'
    #     # print(prepared_data)

    #     response = self._send_request('post', url, headers={'Content-Type': 'application/x-ndjson'}, data=prepared_data)
    #     return response

    
    def create_statement_bach_insert(self, data: dict):
        prepared_data = []
        for _id, value in data.items():
            # prepared_data.append({"_id": _id, "_source": value})

            prepared_data.append({"_id": _id, '_source': data})
            # prepared_data.append(value)
        return prepared_data
            

    def _send_request(self, method: str, url: str, **kwargs):
        r = requests.request(method, url, **kwargs)
        return json.loads(r.text)
    

    def search_field(self, id: str):
        query = {
    "query": {
        "term": {
            "id": {
                "value": id
            }
        }
    }
}
        return self.client.search(index=self.index, body=query)
    

    def search_field11(self, id: str):
        query = {


    "query": {
        "multi_match": {
            "query": "camp",
            "fuzziness": "auto",
            "fields": [
                "actors_names",
                "writers_names",
                "title",
                "description",
                "genres"
            ]
        }
    }
}
        return self.client.search(index=self.index, body=query)
# client = elasticsearch.Elasticsearch("http://localhost:9200")
# print(client.info())
if __name__ == '__main__':
    # es = ElasticSearchLoader()
    # es.create_index('./schema.json', 'new')



    d = ElasticSearchLoader('movies')
    print(d.search_field11(12))