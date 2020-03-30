from datetime import datetime
from elasticsearch import Elasticsearch
from CsvToJson import CsvToJson


class Queries:

    es = Elasticsearch()

    @classmethod
    def firstQuestion(cls, indexx, query):
        print("==================== This Is Answer To Question One ====================")
        res = cls.es.search(index=indexx, body={"from": 0, "size": 100, "query": {"multi_match": {
            "query": query,
            "fields": ["Title", "Body"]
        }}})
        Queries.printResult(res)

    @classmethod
    def secondQuestion(cls, indexx, query):
        print("==================== This Is Answer To Question Two ====================")
        res = cls.es.search(index=indexx, body={
                            "from": 0, "size": 100, "query": {"match": {"Id": query}}})
        Queries.printResult(res)

    @classmethod
    def thirdQuestion(cls, indexx):
        matches = []
        tags = ""
        print("==================== This Is Answer To Question Tree ====================")
        # res = cls.es.search(index=indexx, body={
        #                     "from": 0, "size": 100, "query": {"match": {"Tags": "c++"}}})
        res = cls.es.search(index=indexx, body={
                            "from": 0, "size": 100, "query": {"match_all": {}}})
        for i in res['hits']['hits']:
            tags = i['_source']['Tags']
            content = i['_source']['Title']+" "+i['_source']['Body']
            isInFlag = False
            for j in list(filter(None, tags.replace('<', ' ').replace('>', ' ').strip().split(' '))):
                if(j in content):
                    isInFlag = True

            if(not isInFlag):
                matches.append(i['_source'])

        print("Got %d Hits:" % len(matches))
        for hit in matches:
            print("%(Id)s %(Title)s: %(ParentId)s" % hit)

    @classmethod
    def forthQuestion(cls, indexx, idd, score):
        print("==================== This Is Answer To Question Four ====================")
        res = cls.es.search(index=indexx, body={
                            "from": 0, "size": 300, "query": {"bool": {
                                "must": [
                                    {"match_phrase": {"ParentId": idd}},
                                    {"range": {"Score": {"gte": score,
                                                         "boost": 1.0}}}
                                ]

                            }}})
        Queries.printResult(res)

    @classmethod
    def FiftQuestion(cls, indexx, query):
        print("==================== This Is Answer To Question Fifth ====================")
        res = cls.es.search(index=indexx, body={"from": 0, "size": 100, "query": {"multi_match": {
            "query": query,
            "fields": ["Title", "Body"]
        }}})
        Queries.printResult(res)

    @classmethod
    def sixthQuestion(cls, indexx, query, datee):
        # date_time = datee.strftime("%Y-%d-%m %H:%M:%S")
        # "2018-01-01T00:00:00"

        date_int = int(datee.utcnow().timestamp())*1000
        print("==================== This Is Answer To Question Six ====================")
        res = cls.es.search(index=indexx, body={
                            "from": 0, "size": 300, "query": {"bool": {
                                "must": [
                                    {"multi_match": {
                                        "query": query,
                                        "fields": ["Title", "Body"]
                                    }},
                                    {"range": {
                                        "CreationDate": {
                                            "gte": date_int
                                        }
                                    }}
                                ]

                            }}})
        Queries.printResult(res)

    @classmethod
    def printResult(cls, res):
        print("Got %d Hits:" % res['hits']['total']['value'])
        for hit in res['hits']['hits']:
            print(
                "Id: %(Id)s  Title: %(Title)s: ParentId: %(ParentId)s Score: %(Score)s" % hit["_source"])
