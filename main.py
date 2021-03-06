from datetime import datetime
from elasticsearch import Elasticsearch
from CsvToJson import CsvToJson
from Queries import Queries
from datetime import datetime
from pathlib import Path

es = Elasticsearch()


# es = Elasticsearch(HOST="http://localhost",PORT=9200)

#result = es.indices.delete(index="answer-index")
#result = es.indices.delete(index="question-index")


docs = CsvToJson.convertToArrayDictionary(Path("data/QueryResults.json"))
for i in range(0, len(docs)):
    es.index(index="question-index",
             doc_type="Stackoverflow", id=i, body=docs[i])


docs = CsvToJson.convertToArrayDictionary(Path("data/QueryResults2.json"))
for i in range(0, len(docs)):
    es.index(index="answer-index",
             doc_type="Stackoverflow", id=i, body=docs[i])

#mp = es.indices.get_mapping(index="question-index")
# result = es.indices.exists(index="question-index")
# print(result)

# first Question
Queries.firstQuestion("question-index", "How")

# Second Question
Queries.secondQuestion("question-index", "54596743")

# Third Question
Queries.thirdQuestion("question-index")


# Fourth Question # problem
Queries.forthQuestion("answer-index", 55243660, 1)

# Fift Question
Queries.FiftQuestion("answer-index", "dig")

# Sixth Question ##problem
date_time = datetime(2019, 6, 6)
hamed = date_time.strftime("%Y-%d-%m %H:%M:%S")
Queries.sixthQuestion("answer-index", "yes", date_time)

# res = es.get(index="test-index", id=1)
# print(res['_source'])

# res = es.get(index="test-index", id=2)
# print(res['_source'])

# res = es.get(index="test-index", id=3)
# print(res['_source'])


# es.indices.refresh(index="test-index")
