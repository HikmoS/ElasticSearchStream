from elasticsearch import Elasticsearch
import time

es = Elasticsearch()

class dataProvider():
    def __init__(self,keyword):
        self.keyword = keyword 

    def constructCSVfromElastic(self):
        docx={"query": {
                    "bool": {
                        "must": {
                            "bool" : { "should": [
                                { "match": { "message": "Python" }},
                                { "match": { "message": "JavaScript" }}, 
                                { "match": { "message": "Ruby" }} ]}
                        },
                        "filter": {                            
                            "range" : {
                                "date": {
                                    "from" : "2018-05-19 00:00:00",
                                    "to" : "now",
                                    "format" : "yyyy-MM-dd HH:mm:ss"
                                } 
                            }
                        }
                    }
                }
            }

        res = es.search(index="deneme", doc_type="twitter", body=docx,size=1000)
        print("%d tweets found\n" % res['hits']['total'])

        for doc in res['hits']['hits']:
            print("Author: %s\ndate: %s\nTweet: %s\n" % (doc['_source']['author'],doc['_source']['date'],doc['_source']['message']))

if __name__ == '__main__':
    elasticread = dataProvider("Python")
    elasticread.constructCSVfromElastic()