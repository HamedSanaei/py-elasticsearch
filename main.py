from datetime import datetime
from elasticsearch import Elasticsearch
from CsvToJson import CsvToJson
from Queries import Queries
from datetime import datetime

es = Elasticsearch()


# es = Elasticsearch(HOST="http://localhost",PORT=9200)


# doc = {
#     'author': 'kimchy',
#     'text': 'Elasticsearch: cool. bonsai cool.',
#     'timestamp': datetime.now(),
# }

# result = es.indices.delete(index="test-index")
# print(result)
# result = es.indices.delete(index="test-index")
result = es.indices.delete(index="answer-index")
# result = es.indices.delete(index="question-index")


docs = CsvToJson.convertToArrayDictionary("data/QueryResults.json")
for i in range(0, len(docs)):
    es.index(index="question-index",
             doc_type="Stackoverflow", id=i, body=docs[i])


# result = es.indices.exists(index="question-index")
# print(result)

# first Question
# Queries.firstQuestion("question-index","How")

# Second Question
# Queries.secondQuestion("question-index","54596743")

# Third Question
# Queries.thirdQuestion("question-index")

docs = CsvToJson.convertToArrayDictionary("data/QueryResults2.json")
for i in range(0, len(docs)):
    es.index(index="answer-index",
             doc_type="Stackoverflow", id=i, body=docs[i])

# Fourth Question # problem
#Queries.forthQuestion("answer-index", 55243660, 1)


# Sixth Question
hamed = ""
date_time = datetime(2018, 6, 6)
hamed = date_time.strftime("%Y-%d-%mT%H:%M:%S")
Queries.sixthQuestion("answer-index", "It is lefmvr", date_time)

# res = es.get(index="test-index", id=1)
# print(res['_source'])

# res = es.get(index="test-index", id=2)
# print(res['_source'])

# res = es.get(index="test-index", id=3)
# print(res['_source'])


# es.indices.refresh(index="test-index")

# res = es.search(index="test-index", body={"query": {"match_all": {}}})
# print("Got %d Hits:" % res['hits']['total']['value'])
# for hit in res['hits']['hits']:
#     print("%(Id)s %(Title)s: %(ParentId)s" % hit["_source"])
