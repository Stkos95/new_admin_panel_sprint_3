import requests
import json


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


class ElasticSearchLoader:
    
    def __init__(self, index_name, path_scheme: str = None):
        self.index = index_name

    def _load_schema(self, path_file: str) -> str:
        with open(path_file, 'r') as file:
            schema = file.read()
        return schema

    def create_index(self, schema: str):
        url = f'http://127.0.0.1:9200/{self.index}/'
        r = self._send_request('put', url, headers={'Content-Type': 'application/json'}, data=schema)
        if r.get('status') == 400:
            #logger 
            print('Индекс уже создан.')

    def insert_data(self, data: dict):
        url = f'http://127.0.0.1:9200/{self.index_name}/_doc/'
        return self._send_request('post', url, headers={'Content-Type': 'application/json'}, data=data)
    
    def update_data(self, indx: str, data: dict):
        url = f'http://127.0.0.1:9200/{self.index_name}/_doc/{indx}/'
        return self._send_request('post', url, headers={'Content-Type': 'application/json'}, data=data) 


    def batch_insert_data(self, data: dict):
        url = f'http://127.0.0.1:9200/_bulk?filter_path=items.*.error'
        prepared_data = self.create_statement_bach_insert(data)
        response = self._send_request('post', url, headers={'Content-Type': 'application/x-ndjson'}, data=prepared_data)
        return response.get('errors')

    
    def create_statement_bach_insert(self, data: dict):
        prepared_data = {}
        for _id, value in data.items():
            prepared_data.update({"index":{"_index": self.index, "_id": _id}})
            prepared_data.update(value)
        return prepared_data
            

    def _send_request(self, method: str, url: str, **kwargs):
        r = requests.request(method, url, **kwargs)
        return json.loads(r.text)
    
    
# if __name__ == '__main__':
    # es = ElasticSearchLoader()
    # es.create_index('./schema.json', 'new')



