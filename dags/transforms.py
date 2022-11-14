import re

import pandas as pd

def normalise(string: str)->str:
    return ''.join(c.lower() for c in string if c.isalnum())

def remove_withdrawn_articles(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Drop withdrawn articles from dataframe.
    :param dataframe:
    :return:
    """
    p = re.compile('\s+(This|The) (paper|submission|manuscript|work) (has been|is being|is) withdrawn')
    not_withdrawn = dataframe['abstract'].apply(p.match).isnull()
    return dataframe.loc[not_withdrawn]


def remove_duplicates(dataframe: pd.DataFrame) -> pd.DataFrame:
    titles: pd.Series = dataframe['title'].apply(normalise) + dataframe['authors'].apply(normalise)
    return dataframe.loc[~titles.duplicated()]

def get_authors_and_affiliations(authors_parsed):
    authors = []
    affiliations = []
    for p in authors_parsed:
        try:
            i = p.index('')
            authors.append(p[:i])
            if i == len(p)-1:
                affiliations.append([])
            else:
                affiliations.append(p[i+1:])
        except ValueError:
            authors.append(p)
            affiliations.append([])
            continue
    return authors, affiliations

def extract_name_and_affiliation(l):
    family = l[0] or None
    given = l[1] or None
    affiliation = list(filter(len, l[2:])) or []
    assert len(family) and family is not None
    return dict(family=family, given=given, affiliation=affiliation)

def extract_names_and_affiliations(ls):
    return [
        extract_name_and_affiliation(l)
        for l in ls
    ]

from operator import itemgetter

def latest_version(s):
    return itemgetter('version')(itemgetter(-1)(s))



def clean_dataframe(dataframe):
    dataframe = remove_withdrawn_articles(dataframe)
    dataframe.drop(['comments', 'abstract', 'license', 'update_date', 'report-no'], axis=1, inplace=True)
    dataframe['versions'] = dataframe['versions'].apply(latest_version)
    dataframe['title'] = dataframe['title'].str.replace('\n','')
    dataframe['authors'] = dataframe['authors'].str.replace('\n','')
    dataframe['journal-ref'] = dataframe['journal-ref'].str.replace('\n','')
    dataframe = remove_duplicates(dataframe)

    authors, affs = get_authors_and_affiliations(dataframe['authors_parsed'])
    dataframe['authors_parsed'] = dataframe['authors_parsed'].apply(extract_names_and_affiliations)
    dataframe['authors'] = authors
    dataframe['affiliation'] = affs
    return dataframe
