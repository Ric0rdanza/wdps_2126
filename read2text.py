from pyspark import SparkContext, SparkConf
from elasticsearch import Elasticsearch
import trident
import sys
import requests
from functools import reduce
from io import StringIO
import re
from html.parser import HTMLParser
import spacy
import nltk
from nltk.corpus import stopwords
import math
import json

KEYNAME = "WARC-Record-ID"
KBPATH='assets/wikidata-20200203-truthy-uri-tridentdb'
nltk.download('stopwords')
nltk.download('maxent_ne_chunker')
sw_eng = stopwords.words('english')

SPACY_MODEL = None


def get_spacy_model():
    global SPACY_MODEL
    if not SPACY_MODEL:
        _model = spacy.load('en_core_web_lg')
        # FIX https://github.com/explosion/spaCy/issues/922
        _model.vocab.add_flag(
            lambda s: s.lower() in spacy.lang.en.stop_words.STOP_WORDS,
            spacy.attrs.IS_STOP
        )
        SPACY_MODEL = _model
    return SPACY_MODEL


class WarcRecord:
    def __init__(self, web_arch_record: str):
        self.id = None
        self.payload = None
        self.broken = None
        # self.ner = None
        self._parse(web_arch_record)

    def _parse(self, web_arch_record):
        buffer = StringIO(web_arch_record.strip())
        # Parsing headers
        while True:
            line = buffer.readline().strip()
            if line == '':
                break
            if self.id is None and 'WARC-TREC-ID' in line:
                self.id = line.split('WARC-TREC-ID:')[1].strip()
        if self.id is None:
            self.broken = True
            return None
        # Maybe skip another set of headers
        line = buffer.readline().strip()
        if line.startswith('HTTP/'):
            line = ''
            while True:
                if buffer.readline().strip() == '':
                    break
        # Rest is payload
        self.payload = line + buffer.read().strip()
        self.broken = False


class TextExtractor(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self._inside_script = False
        self._inside_style = False
        self._inside_title = False
        self._words = []
        self._word_regex = re.compile('\w+')  # Regex for words..

    def handle_starttag(self, tag, attrs):
        if tag == 'script':
            self._inside_script = True
        elif tag == 'style':
            self._inside_style = True
        elif tag == 'title':
            self._inside_title = True

    def handle_endtag(self, tag):
        self._inside_script = False
        self._inside_style = False
        self._inside_title = False

    def handle_data(self, data):
        if self._inside_script or self._inside_style or self._inside_title:
            return
        words = self._word_regex.findall(data)
        if len(words) == 0:
            return
        # print(words)
        if 0 < len(words) <= 2:
            self._words.append(' '.join(words))
        else:
            for word in words:
                self._words.append(word)

    @staticmethod
    def get_all_words(text: str) -> list:
        extractor = TextExtractor()
        extractor.feed(text)
        return extractor._words


def get_entities(record):
    _, warc_arch = record
    if not record:
        return
    warc_record = WarcRecord(warc_arch)
    warc_payload = warc_record.payload
    if warc_record.broken:
        return
    words = TextExtractor.get_all_words(warc_payload)
    words = [word for word in words if word not in sw_eng]
    sentence = ""
    if words:
        if len(words) > 1:
            sentence = reduce(lambda a, b: a + " " + b, words)
    nlp = get_spacy_model()
    doc = nlp(sentence)
    sentence = ""
    nerList = {}
    disregard_ne_list = ("DATE", "TIME", "PERCENT", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL")
    for X in doc.ents:
        if X.label_ not in disregard_ne_list:
            nerList[X.text] = X.label_

    yield (warc_record.id, nerList)

def get_elasticsearch(record):
    tuples = []
    for i in record:
        query = i
        e = Elasticsearch("http://localhost:9200")
        p = { "query" : { "query_string" : { "query" : query }}}
        response = e.search(index="wikidata_en", body=json.dumps(p), request_timeout=1000)
        result = {}
        id_max = {}
        filtered_result = {}
        max_score = 0
        if response:
            for hit in response['hits']['hits']:
                freebase_id = hit['_id']
                label = hit.get('_source', {}).get('schema_name')
                score = hit.get('_score', 0)
                if result.get(freebase_id) is None:
                    result[freebase_id] = ({
                        'label': label,
                        'score': score,
                        'facts': 0,
                        'rank': 0
                    })
                else:
                    score_1 = max(result[freebase_id]['score'], score)
                    result[freebase_id]['score'] = score_1
                if result[freebase_id]['score'] > max_score:
                    max_score = result[freebase_id]['score']
            for f_id, res in result.items():
                if f_id and res:
                    if res['score'] >= max_score:
                        id_max[f_id] = res
            tuples.append([i, id_max])
    yield tuples

sparql_query = """
    SELECT DISTINCT * WHERE {
        %s ?p ?o.
    }
    """    
    
def get_kbdata(record):
    tuples = []
    db = trident.Db(KBPATH)
    for i in record:
        entity = i[0]
        for key in i[1]:
            query = sparql_query % key
            results = db.sparql(query)
            response = json.loads(results)
            if response:
                n = int(response.get('stats', {}).get('nresults', 0))
                i[1][key]['facts'] = n
                if n != 0:
                    i[1][key]['rank'] = math.log(n) * i[1][key]['score']
                else:
                    i[1][key]['rank'] = 0
        tuples.append([entity, i[1]])
    yield tuples

def get_linkedent(record):
    linked_ent = {}
    tuples = []
    for i in record:
        linked_ent = dict()
        entity = i[0]
        if i[1].items() is not None:
            linked_ent = dict(sorted(i[1].items(), key=lambda x: (x[1]['rank']), reverse=True)[:1])
        tuples.append([entity, linked_ent])
    yield tuples

def get_output(record):
    line = ''
    if record:
        if record[1]:
            for i in record[1]:
                if i[0] and i[1]:
                    for key in i[1]:
                        if key:
                            line += record[0] + "\t" + i[0] + "\t" + key + "\n"
    return line    
    
if __name__ == '__main__':
    import sys
    try:
        _, INPUT = sys.argv
    except Exception as e:
        print('Usage: python starter-code.py INPUT')
        sys.exit(0)

    sc = SparkContext().getOrCreate()
    SPACY = spacy.load('en_core_web_sm')
    
    rdd = sc.newAPIHadoopFile(INPUT,
        "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
        "org.apache.hadoop.io.LongWritable",
        "org.apache.hadoop.io.Text",
        conf={"textinputformat.record.delimiter": "WARC/1.0"})

    rdd_pairs = rdd.flatMap(get_entities)
    rdd_esearched = rdd_pairs.flatMapValues(get_elasticsearch)
    rdd_kbdat = rdd_esearched.flatMapValues(get_kbdata)
    rdd_linkedent = rdd_kbdat.flatMapValues(get_linkedent)
    result = rdd_linkedent.map(get_output)
    print(result.take(5))
    
#result = result.coalesce(1,True).saveAsTextFile("/app/assignment/test1.tsv")
