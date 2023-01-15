import pandas as pd
import time
import numpy as np
import requests
from scholarly import scholarly
from habanero import Crossref
from typing import Tuple

Crossref(mailto = "kohv.kea@gmail.com")

class CrossRefFieldExtractor:
    fields = ['DOI', 'title', 'author', 'type', 'publisher', 'is-referenced-by-count', 'container-title', 'subject', 'published', 'reference']

    def __call__(self, response):
        data = {}
        for field in CrossRefFieldExtractor.fields:
            if response.get(field) is None:
                data[field.lower()] = None
                continue


            if field in ['container-title', 'title', 'subject']:
                try:
                    data[field.lower()] = response.get(field)[0]
                except IndexError:
                    data[field.lower()] = None
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

def process_names(nameslist):
    '''
    Define names for querying scholarly. Search won't work if name contains dots.
    '''
    names = nameslist[::-1]
    *first_names, last_name = names

    initial_or_first_name = ' '.join(first_names).split(' ')[0]
    initial_or_first_name = ''.join(filter(str.isalnum, initial_or_first_name))
    fullname = initial_or_first_name + ' ' + last_name
    return fullname


def find_gender(full_name, first_name, last_name):
    '''
    Query AMiner Gender API for author's gender
    '''
    # If arxiv dataset had the author's first_name (not initial), us that for querying
    if str.isalnum(first_name) == False:
        if full_name.count(" ") == 1:
            first_name, last_name = full_name.split(' ')
        elif full_name.count(" ") == 2:
            # For finding names where the middle name is included as an inital
            first_name, middle, last_name = full_name.split(' ')

    try:
        url = f'https://innovaapi.aminer.cn/tools/v1/predict/gender?name={first_name}+{last_name}&org='
        r = requests.get(url=url)
        data = r.json()
        gender = data['data']['Final']['gender']
        print(f'Finding gender for {full_name}, gender is {gender}')

        if gender == 'UNKNOWN':
            gender = 'Unknown'
    except:
        gender = 'Unknown'
    return gender



def assign_genders(authors_merged: pd.Series)->pd.Series:
    for authorlist in authors_merged:
        for author in authorlist:
            author['full_name'] = process_names([str(author['family']), str(author['given'])])

            author['gender'] = "Unknown"
            if str.isalnum(str(author['given'])) == True:
                 # If scholarly request is not needed, find gender
                author['gender'] = find_gender(author['full_name'], str(author['given']), str(author['family']))



def mock_assign_genders(authors_merged: pd.Series)->pd.Series:
    for authorlist in authors_merged:
        for author in authorlist:
            gender_number = sum(author['given'].encode('utf8')) % 5
            if gender_number == 0:
                gender = 'Unknown'
            elif gender_number & 1:
                gender = 'Female'
            else:
                gender =  'Male'
            author['gender'] = gender

def get_names_gender(authors_merged: pd.Series)->pd.Series:
    '''
    Takes authors from the 'authors_merged' field and
    adds full name and gender if author only has  initial(s)
    '''
    for authorlist in authors_merged:
        for author in authorlist:

            # If the author's given name contains characters that are not alphanumeric
            # (e.g. dot, indicating that only the initial is known), search Google Scholar for the full name of the author
            if str.isalnum(str(author['given'])) == False:
                name = process_names([str(author['family']), str(author['given'])])
                print(f'Full name search for {name}')
                try:
                    search_query = scholarly.search_author(name)
                    first_author_result = next(search_query)
                    author['full_name'] = first_author_result['name']

                    try:
                        gender = find_gender(author['full_name'],str(author['given']), str(author['family']))
                        if gender != 'UNKNOWN':
                            author['gender'] = gender
                        else:
                            author['gender'] = 'Unknown'
                    except:
                        author['gender'] = 'Unknown'

                except:
                    author['full_name'] = name


            print(author)


class ReferenceInfo:
    def get(self, references):
        dois = []
        for r in references:
            if r.get('DOI') is not None:
                dois.append(r['DOI'])
        return dois

ri = ReferenceInfo()
def process_crossref_work(authors=None, doi=None, title=None):
    extract = CrossRefFieldExtractor()
    cr = Crossref(mailto='joosephook@gmail.com')
    ri = ReferenceInfo()

    result = cr.works(limit=1, query_author=authors, doi=doi, query_title=title)
    if result['status'] == 'ok' and len(result['message']['items']) > 0:
        item = result['message']['items'][0]
        if item.get('reference', {}):
            ids = ri.get(item['reference'])
            refs = cr.works(ids=ids, warn=True) # don't throw exception if HTTP request fails
            if not isinstance(refs, list):
                refs = [refs]
            references = []
            for r in refs:
                if r is not None and r['status'] == 'ok':
                    references.append(extract(r['message']))
            item['reference'] = references
            print('before:', item['author'])
            print('after:', item['author'])
        else:
            item['reference'] = []
        aux = extract(item)
    else:
        aux = {}
    return aux

from openalex import process_openalex_work
def enrich(dataframe: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    extra = []
    for t in dataframe.itertuples():
        authors = [a['family'] for a in t.authors_parsed]

        aux = process_openalex_work(authors=authors, doi=t.doi, title=t.title)

        if len(aux) == 0:
            aux = process_crossref_work(authors=authors, doi=t.doi, title=t.title)

        if len(aux) == 0:
            print('Failed to find match for')
            print(t)
        else:
            aux['author'] = list(filter(lambda x: x.get("family", False), aux['author']))
        extra.append(aux)

    succeeded = np.array(list(map(bool, extra)))
    failed    =~succeeded
    print('Failed:', sum(failed))
    enriched = dataframe.drop(['doi', 'title'], axis=1)
    enriched = pd.concat([enriched, pd.DataFrame.from_records(extra, coerce_float=False)], axis=1, ignore_index=False)
    enriched = enriched.loc[succeeded]
    enriched = merge_authorlists(enriched)
    # assign_genders(enriched['authors_merged'])
    # get_names_gender(enriched['authors_merged'])
    mock_assign_genders(enriched['authors_merged'])
    enriched = enriched.drop(['authors_parsed', 'author'], axis=1)

    return enriched, dataframe.loc[failed]


def n_utf8_bytes(x: str):
    return len(x.encode('utf8'))


def merge_author_names(old, new):
    old_name_score = int(len(old.get('given', '')) > 0) + int(len(old.get('family', '')))
    new_name_score = int(len(new.get('given', '')) > 0) + int(len(new.get('family', '')))

    if new_name_score > old_name_score:
        given = new.get('given', '')
        family = new.get('family', '')
        return dict(given=given, family=family)
    elif new_name_score < old_name_score:
        given = old.get('given', '')
        family = old.get('family', '')
        return dict(given=given, family=family)
    else:
        given = max(old.get('given', ''), new.get('given', ''), key=n_utf8_bytes)
        family = max(old.get('family', ''), new.get('family', ''), key=n_utf8_bytes)
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

        if new is None or (isinstance(new, float) and np.isnan(new)):
            print('Failed getting new authors for', t.title)
            merged.append(old)
            continue

        authorlist = []

        new = sorted(new, key=lambda x: x["family"])
        old = sorted(old, key=lambda x: x["family"])

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

    record = pd.DataFrame.from_records(lines[25:35])
    record = clean_dataframe(record)
    success, failure = enrich(record)
    # extra.to_csv('enriched.csv', index=False)