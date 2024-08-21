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
# r = requests.put(url='http://127.0.0.1:9200/movies', headers={'Content-Type': 'application/json'}, data=json.dumps(schema))
# print(json.loads(r.text))


z = """
    {
        "actors_names": ["1", "2"]
    }

"""

r = requests.post('http://127.0.0.1:9200/movies/_doc/', headers={'Content-Type': 'application/json'}, data = z)
print(json.loads(r.text))