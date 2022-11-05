from habanero import Crossref
import json

# example record without doi, journal, publishing date
line = r'{"id":"0704.0002","submitter":"Louis Theran","authors":"Ileana Streinu and Louis Theran","title":"Sparsity-certifying Graph Decompositions","comments":"To appear in Graphs and Combinatorics","journal-ref":null,"doi":null,"report-no":null,"categories":"math.CO cs.CG","license":"http://arxiv.org/licenses/nonexclusive-distrib/1.0/","abstract":"  We describe a new algorithm, the $(k,\\ell)$-pebble game with colors, and use\nit obtain a characterization of the family of $(k,\\ell)$-sparse graphs and\nalgorithmic solutions to a family of problems concerning tree decompositions of\ngraphs. Special instances of sparse graphs appear in rigidity theory and have\nreceived increased attention in recent years. In particular, our colored\npebbles generalize and strengthen the previous results of Lee and Streinu and\ngive a new proof of the Tutte-Nash-Williams characterization of arboricity. We\nalso present a new decomposition that certifies sparsity based on the\n$(k,\\ell)$-pebble game with colors. Our work also exposes connections between\npebble game algorithms and previous sparse graph algorithms by Gabow, Gabow and\nWestermann and Hendrickson.\n","versions":[{"version":"v1","created":"Sat, 31 Mar 2007 02:26:18 GMT"},{"version":"v2","created":"Sat, 13 Dec 2008 17:26:00 GMT"}],"update_date":"2008-12-13","authors_parsed":[["Streinu","Ileana",""],["Theran","Louis",""]]}'
record = json.loads(line)
print(record.keys())

query = dict(
    query_author=record['authors_parsed'],
    query_doi=record['doi'],
    query_title=record['title'],
)
cr = Crossref()
response = cr.works( limit=1, **query)

fields = ['DOI', 'type', 'publisher', 'is-referenced-by-count', 'title', 'container-title', 'subject', 'published']
from operator import itemgetter
def extract(item, fields):
    return dict(zip(fields, itemgetter(*fields)(item)))

aux = extract(response['message']['items'][0], fields)
from pprint import pprint
pprint(aux)