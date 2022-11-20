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
    #df = df.sort_values(by=['doi','update_date']).drop_duplicates(subset=(['title','authors']), keep='last')
    # Praegune vist ei arvesta sellega, et doi-ga recordit eelistada?
    titles: pd.Series = dataframe['title'].apply(normalise) + dataframe['authors'].apply(normalise)
    return dataframe.loc[~titles.duplicated()]

def extract_name_and_affiliation(l):
    family = l[0] or ''
    given = l[1] or ''
    affiliation = list(filter(len, l[2:])) or []
    return dict(family=family, given=given, affiliation=[dict(name=affiliation)])

def extract_names_and_affiliations(ls):
    return list(map(extract_name_and_affiliation, ls))

from operator import itemgetter

def latest_version(s):
    return itemgetter('version')(itemgetter(-1)(s))

def clean_dataframe(dataframe):
    dataframe = remove_withdrawn_articles(dataframe)
    dataframe.drop(['comments', 'abstract', 'license', 'update_date', 'report-no'], axis=1, inplace=True)
    dataframe['versions'] = dataframe['versions'].apply(latest_version)
    dataframe.dropna(subset=['authors'])
    dataframe['title'] = dataframe['title'].str.replace('\n','')
    dataframe['authors'] = dataframe['authors'].str.replace('\n','')
    dataframe['journal-ref'] = dataframe['journal-ref'].str.replace('\n','')
    dataframe = remove_duplicates(dataframe)

    dataframe['authors_parsed'] = dataframe['authors_parsed'].apply(extract_names_and_affiliations)
    return dataframe
