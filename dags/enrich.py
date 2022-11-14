import pandas as pd
import orjson
from habanero import Crossref

"""
{'DOI': '10.1016/s0370-2693(96)01409-8',
 'author': [{'affiliation': [],
             'family': 'Fioravanti',
             'given': 'D',
             'sequence': 'first'},
            {'affiliation': [],
             'family': 'Mariottini',
             'given': 'A',
             'sequence': 'additional'},
            {'affiliation': [],
             'family': 'Quattrini',
             'given': 'E',
             'sequence': 'additional'},
            {'affiliation': [],
             'family': 'Ravanini',
             'given': 'F',
             'sequence': 'additional'}],
 'container-title': ['Physics Letters B'],
 'is-referenced-by-count': 128,
 'published': {'date-parts': [[1997, 1]]},
 'publisher': 'Elsevier BV',
 'subject': ['Nuclear and High Energy Physics'],
 'title': ['Excited state Destri-De Vega equation for sine-Gordon and '
           'restricted sine-Gordon models'],
 'type': 'journal-article'}
"""

class CrossRefFieldExtractor:
    fields = ['DOI', 'title', 'author', 'type', 'publisher', 'is-referenced-by-count', 'container-title', 'subject', 'published']

    def __call__(self, response):
        data = {}
        for field in CrossRefFieldExtractor.fields:
            if response.get(field) is None:
                data[field.lower()] = None
                continue


            if field in ['container-title', 'title', 'subject']:
                data[field.lower()] = response.get(field)[0]
            elif field == 'published':
                try:
                    year, *rest = response.get(field)['date-parts'][0]
                    data['published-year']  = year
                    if len(rest) > 0:
                        month = rest.pop(0)
                        data['published-month'] = month
                    else:
                        data['published-month'] = None
                except KeyError:
                    data['published-year']  = None
                    data['published-month'] = None
            else:
                data[field.lower()] = response.get(field)
        return data


import time
def enrich(dataframe: pd.DataFrame) -> pd.DataFrame:
    extract = CrossRefFieldExtractor()
    cr = Crossref()

    extra = []
    for t in dataframe.itertuples():
        authors = [a['family'] for a in t.authors_parsed]
        result = cr.works(limit=1, query_author=authors, doi=t.doi, query_title=t.title)
        aux = extract(result['message']['items'][0])
        extra.append(aux)
        time.sleep(0.1)
    dataframe.drop(['doi', ], axis=1, inplace=True)
    return pd.concat([dataframe, pd.DataFrame.from_records(extra)], axis=1)

def merge_authorlists(dataframe : pd.DataFrame) -> pd.DataFrame:
    merged = []
    for t in dataframe.itertuples(index=False):
        new = t.author
        old = t.authors_parsed
        authorlist = []
        for n, o in zip(new, old):
            # TODO: merge author lists
            if ' ' not in o['family']:
            family = o['family']
            given = max(o['given'], n['given'], key=len)
            affiliations = max(o['affiliation'], n['affiliation'], key=lambda x: len(''.join(x)))
            authorlist.append(dict(family=family, given=given, affiliations=affiliations))
        merged.append(authorlist)

if __name__ == '__main__':
    lines = [
        orjson.loads(s)
        for s in open('/home/joosep/Downloads/arxiv-dataset/arxiv-metadata-oai-snapshot.json.aa', 'r')
    ]
    from transforms import clean_dataframe

    record = pd.DataFrame.from_records(lines[:40])
    record = clean_dataframe(record)
    extra = enrich(record)
    extra['merged'] = merge_authorlists(extra)

    extra.to_csv('enriched.csv', index=False)

