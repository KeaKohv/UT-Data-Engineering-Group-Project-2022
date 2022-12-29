import pandas as pd
import time
import requests
from scholarly import scholarly
from pprint import pprint
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


def assign_genders(authors_merged: pd.Series)->pd.Series: # POLE VAJA
    for authorlist in authors_merged:
        for author in authorlist:
            author['gender'] = None


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


def find_gender(full_name):
    '''
    Query AMiner Gender API for author's gender
    '''
    
    if full_name.count(" ") == 1:
        first_name, last_name = full_name.split(' ')
    elif full_name.count(" ") == 2:
        # For finding names where the middle name is included as an inital
        first_name, middle, last_name = full_name.split(' ')
    
    # Hetkel ei tööta siis, kui perekonnanimi on mitme sõnaga nt "Wanderley Dantas dos Santos".
    # Samas see full_name on sisend scholarly-st ja scholarly ei ütle, mis on eesnimi ja mis perekonnanimi.
    # Seega ei oska sellist case-i siia kirja panna.

    try:
        url = f'https://innovaapi.aminer.cn/tools/v1/predict/gender?name={first_name}+{last_name}&org='
        r = requests.get(url=url)
        data = r.json()
        gender = data['data']['Final']['gender']
    except:
        gender = 'UNKNOWN'
    return gender



def get_names_aff_gender(authors_merged: pd.Series)->pd.Series:
    '''
    Takes authors from the 'authors_merged' field and
    adds full name, affiliation and gender for each author
    '''
    for authorlist in authors_merged:
        for author in authorlist:
            name = process_names([str(author['family']), str(author['given'])])
            print('Full name, aff and gender search:')
            print(name)
            try:
                search_query = scholarly.search_author(name)
                first_author_result = next(search_query) # Esimene vaste ei pruugi alati õige olla ja ei pruugi üldse vastet olla
                author['full_name'] = first_author_result['name']
                author['affiliation'] = first_author_result['affiliation'] # Võib olla mitu affiliationit
            except:
                author['full_name'] = 'None'

                # Kui scholarly ei leia, siis jääb arxiv andmestiku info
                if author['affiliation'] != None:
                    author['affiliation'] = str(author['affiliation'])

            try:
                # Praegu küsib ainult siis, kui scholarly-s oli autor olemas
                # Võiks lisada selle, et kui full_name pole, aga given on olemas (pole initsiaal), siis küsib ka
                if author['full_name'] != 'None':
                    gender = find_gender(author['full_name'])
                    if gender != 'UNKNOWN':
                        author['gender'] = gender
                    else:
                        author['gender'] = 'None'
                else:
                        author['gender'] = 'None'
            except:
                author['gender'] = 'None'

            print(author)
            time.sleep(0.1)


class ReferenceInfo:
    def get(self, references):
        dois = []
        for r in references:
            if r.get('DOI') is not None:
                dois.append(r['DOI'])
        return dois

ri = ReferenceInfo()
def enrich(dataframe: pd.DataFrame) -> pd.DataFrame:
    extract = CrossRefFieldExtractor()
    cr = Crossref()

    extra = []
    for t in dataframe.itertuples():
        authors = [a['family'] for a in t.authors_parsed]
        result = cr.works(limit=1, query_author=authors, doi=t.doi, query_title=t.title)
        if result['status'] == 'ok':
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
            else:
                item['reference'] = []
        else:
            item = {}
            
        aux = extract(item)
        extra.append(aux)
        time.sleep(0.1)
    dataframe.drop(['doi', 'title'], axis=1, inplace=True)
    dataframe = pd.concat([dataframe, pd.DataFrame.from_records(extra)], axis=1)
    dataframe = merge_authorlists(dataframe)
    assign_genders(dataframe['authors_merged'])

    # Kea added:
    # get_names_aff_gender(dataframe['authors_merged'])
    # dataframe.drop(['authors','authors_parsed','categories', 'journal-ref', 'journal-ref', 'submitter', 'author'], axis=1, inplace=True)

    return dataframe


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
        authorlist = []
        if new is None:
            new = {}
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
    extra = enrich(record)
    extra['merged'] = merge_authorlists(extra)
    # extra.to_csv('enriched.csv', index=False)
"""
[
{'key': 'PhysRevLett.99.087402Cc1R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.2.393'}
{'key': 'PhysRevLett.99.087402Cc2R1', 'doi-asserted-by': 'crossref', 'volume-title': 'Many-Particle Physics', 'author': 'G.\u2009D. Mahan', 'year': '2000', 'DOI': '10.1007/978-1-4757-5714-9'}
{'key': 'PhysRevLett.99.087402Cc3R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1126/science.1102896'}
{'key': 'PhysRevLett.99.087402Cc4R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nature04233'}
{'key': 'PhysRevLett.99.087402Cc5R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nature04235'}
{'key': 'PhysRevLett.99.087402Cc6R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.96.136806'}
{'key': 'PhysRevLett.99.087402Cc7R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nphys245'}
{'key': 'PhysRevLett.99.087402Cc8R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nmat1846'}
{'key': 'PhysRevLett.99.087402Cc9R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.98.166802'}
{'key': 'PhysRevLett.99.087402Cc10R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1143/JPSJ.75.124701'}
{'key': 'PhysRevLett.99.087402Cc11R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.75.045404'}
{'key': 'PhysRevLett.99.087402Cc12R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.97.266407'}
{'key': 'PhysRevLett.99.087402Cc13R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRev.104.666'}
{'key': 'PhysRevLett.99.087402Cc14R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.23.848'}
{'key': 'PhysRevLett.99.087402Cc14R2', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.5.566'}
{'key': 'PhysRevLett.99.087402Cc15R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1063/1.91815'}
{'key': 'PhysRevLett.99.087402Cc15R2', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.53.16481'}
{'key': 'PhysRevLett.99.087402Cc16R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1143/JPSJ.76.024712'}
{'key': 'PhysRevLett.99.087402Cc17R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.97.187401'}
{'key': 'PhysRevLett.99.087402Cc18R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1021/nl061420a'}
{'key': 'PhysRevLett.99.087402Cc19R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1021/nl061702a'}
{'key': 'PhysRevLett.99.087402Cc20R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.75.125430'}
{'key': 'PhysRevLett.99.087402Cc21R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.97.266405'}
{'key': 'PhysRevLett.99.087402Cc22R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.75.155430'}
{'key': 'PhysRevLett.99.087402Cc23R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.93.185503'}
{'key': 'PhysRevLett.99.087402Cc24R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nmat1849'}
{'key': 'PhysRevLett.99.087402Cc25R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRev.71.622'}
{'key': 'PhysRevLett.99.087402Cc26R1', 'doi-asserted-by': 'crossref', 'volume-title': 'Physical Properties of Carbon Nanotubes', 'author': 'R. Saito', 'year': '1998', 'DOI': '10.1142/p080'}
{'key': 'PhysRevLett.99.087402Cc27R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1143/JPSJ.74.777'}
]
"""
