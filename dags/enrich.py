import pandas as pd
from habanero import Crossref

class CrossRefFieldExtractor:
    fields = ['DOI', 'title', 'author', 'type', 'publisher', 'is-referenced-by-count', 'container-title', 'subject', 'published', 'reference']

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

def assign_genders(authors_merged: pd.Series)->pd.Series:
    for authorlist in authors_merged:
        for author in authorlist:
            author['gender'] = None

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
    dataframe.drop(['doi', 'title'], axis=1, inplace=True)
    dataframe = pd.concat([dataframe, pd.DataFrame.from_records(extra)], axis=1)
    dataframe = merge_authorlists(dataframe)
    assign_genders(dataframe['authors_merged'])
    return dataframe


def n_utf8_bytes(x: str):
    return len(x.encode('utf8'))


def merge_author_names(old, new):
    old_name_score = int(len(old['given']) > 0) + int(len(old['family']))
    new_name_score = int(len(new['given']) > 0) + int(len(new['family']))

    if new_name_score > old_name_score:
        given = new['given']
        family = new['family']
        return dict(given=given, family=family)
    elif new_name_score < old_name_score:
        given = old['given']
        family = old['family']
        return dict(given=given, family=family)
    else:
        given = max(old['given'], new['given'], key=n_utf8_bytes)
        family = max(old['family'], new['family'], key=n_utf8_bytes)
        return dict(given=given, family=family)

def merge_author_affiliations(old, new):
    if len(old['affiliation']) > 0:
        old = old['affiliation'].pop(0).get('name')
    else:
        old = ''

    if len(new['affiliation']) > 0:
        new = new['affiliation'].pop(0).get('name')
    else:
        new = ''

    if len(old) == 0 and len(new) == 0:
        return dict(affiliation=None)
    elif len(old) > len(new):
        return dict(affiliation=old)
    elif len(old) < len(new):
        return dict(affiliation=new)
    else:
        return dict(affiliation=max(old, new, key=n_utf8_bytes))



def merge_authorlists(dataframe : pd.DataFrame) -> pd.DataFrame:
    merged = []
    for t in dataframe.itertuples(index=False):
        new = t.author
        old = t.authors_parsed
        authorlist = []
        for n, o in zip(new, old):
            authors = merge_author_names(o, n)
            authors.update(merge_author_affiliations(o, n))
            authorlist.append(authors)
        merged.append(authorlist)
    dataframe['authors_merged'] = merged
    return dataframe

if __name__ == '__main__':
    import orjson

    lines = [
        orjson.loads(s)
        for s in open('/home/joosep/Downloads/archive/arxiv-metadata-oai-snapshot.json', 'r')
    ]
    from transforms import clean_dataframe

    record = pd.DataFrame.from_records(lines[25:30])
    record = clean_dataframe(record)
    extra = enrich(record)
    # extra['merged'] = merge_authorlists(extra)
    # extra.to_csv('enriched.csv', index=False)

