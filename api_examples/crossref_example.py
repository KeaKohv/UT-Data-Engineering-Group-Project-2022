from habanero import Crossref
from pprint import pprint
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

fields = ['DOI', 'title', 'author', 'type', 'publisher', 'is-referenced-by-count', 'container-title', 'subject', 'published']
def extract(item, fields):
    return {
        field:item.get(field)
        for field in fields
    }

def authors(parsed):
    """
    Extract authors from authors_parsed field, ignoring affiliations
    :param parsed:
    :return:
    """
    authors = []
    for p in parsed:
        author = []
        for t in p:
            if len(t) > 0:
                author.append(t)
            else:
                break
        authors.append(author)
    return authors

aux = extract(response['message']['items'][0], fields)
pprint(aux)

line = r"""{"id":"hep-th/9608091","submitter":"Francesco Ravanini","authors":"D. Fioravanti, A. Mariottini, E. Quattrini and F. Ravanini (Bologna\n  Univ. and INFN)","title":"Excited State Destri - De Vega Equation for Sine-Gordon and Restricted\n  Sine-Gordon Models","comments":"Latex, 12 pages","journal-ref":"Phys.Lett.B390:243-251,1997","doi":"10.1016/S0370-2693(96)01409-8","report-no":"DFUB 96-24","categories":"hep-th","license":null,"abstract":"  We derive a generalization of the Destri - De Vega equation governing the\nscaling functions of some excited states in the Sine-Gordon theory. In\nparticular configurations with an even number of holes and no strings are\nanalyzed and their UV limits found to match some of the conformal dimensions of\nthe corresponding compactified massless free boson. Quantum group reduction\nallows to interpret some of our results as scaling functions of excited states\nof Restricted Sine-Gordon theory, i.e. minimal models perturbed by phi_13 in\ntheir massive regime. In particular we are able to reconstruct the scaling\nfunctions of the off-critical deformations of all the scalar primary states on\nthe diagonal of the Kac-table.\n","versions":[{"version":"v1","created":"Wed, 14 Aug 1996 16:31:26 GMT"}],"update_date":"2010-11-19","authors_parsed":[["Fioravanti","D.","","Bologna\n  Univ. and INFN"],["Mariottini","A.","","Bologna\n  Univ. and INFN"],["Quattrini","E.","","Bologna\n  Univ. and INFN"],["Ravanini","F.","","Bologna\n  Univ. and INFN"]]}"""
record = json.loads(line)
query = dict(
    query_author=authors(record['authors_parsed']),
    doi=record['doi'],
    query_title=record['title'],
)
response = cr.works( limit=1, **query)
pprint(response['message']['items'])
aux = extract(response['message']['items'][0], fields)
pprint(aux)

