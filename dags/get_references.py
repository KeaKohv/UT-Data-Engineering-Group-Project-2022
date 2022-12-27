"""

"""
from habanero import Crossref, WorksContainer

import operator as op

class ReferenceInfo:
    keys = ['DOI', 'author', 'year']

    def get(self, references):
        dois = []
        for r in references:
            if r.get('DOI') is not None:
                dois.append(r['DOI'])
        return dois

if __name__ == '__main__':
    refs = [
        {'key': 'PhysRevLett.99.087402Cc1R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.2.393'},
        {'key': 'PhysRevLett.99.087402Cc2R1', 'doi-asserted-by': 'crossref', 'volume-title': 'Many-Particle Physics', 'author': 'G.\u2009D. Mahan', 'year': '2000', 'DOI': '10.1007/978-1-4757-5714-9'},
        {'key': 'PhysRevLett.99.087402Cc3R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1126/science.1102896'},
        {'key': 'PhysRevLett.99.087402Cc4R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nature04233'},
        {'key': 'PhysRevLett.99.087402Cc5R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nature04235'},
        {'key': 'PhysRevLett.99.087402Cc6R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.96.136806'},
        {'key': 'PhysRevLett.99.087402Cc7R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nphys245'},
        {'key': 'PhysRevLett.99.087402Cc8R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nmat1846'},
        {'key': 'PhysRevLett.99.087402Cc9R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.98.166802'},
        {'key': 'PhysRevLett.99.087402Cc10R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1143/JPSJ.75.124701'},
        {'key': 'PhysRevLett.99.087402Cc11R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.75.045404'},
        {'key': 'PhysRevLett.99.087402Cc12R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.97.266407'},
        {'key': 'PhysRevLett.99.087402Cc13R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRev.104.666'},
        {'key': 'PhysRevLett.99.087402Cc14R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.23.848'},
        {'key': 'PhysRevLett.99.087402Cc14R2', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.5.566'},
        {'key': 'PhysRevLett.99.087402Cc15R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1063/1.91815'},
        {'key': 'PhysRevLett.99.087402Cc15R2', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.53.16481'},
        {'key': 'PhysRevLett.99.087402Cc16R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1143/JPSJ.76.024712'},
        {'key': 'PhysRevLett.99.087402Cc17R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.97.187401'},
        {'key': 'PhysRevLett.99.087402Cc18R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1021/nl061420a'},
        {'key': 'PhysRevLett.99.087402Cc19R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1021/nl061702a'},
        {'key': 'PhysRevLett.99.087402Cc20R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.75.125430'},
        {'key': 'PhysRevLett.99.087402Cc21R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.97.266405'},
        {'key': 'PhysRevLett.99.087402Cc22R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevB.75.155430'},
        {'key': 'PhysRevLett.99.087402Cc23R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRevLett.93.185503'},
        {'key': 'PhysRevLett.99.087402Cc24R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1038/nmat1849'},
        {'key': 'PhysRevLett.99.087402Cc25R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1103/PhysRev.71.622'},
        {'key': 'PhysRevLett.99.087402Cc26R1', 'doi-asserted-by': 'crossref', 'volume-title': 'Physical Properties of Carbon Nanotubes', 'author': 'R. Saito', 'year': '1998', 'DOI': '10.1142/p080'},
        {'key': 'PhysRevLett.99.087402Cc27R1', 'doi-asserted-by': 'publisher', 'DOI': '10.1143/JPSJ.74.777'},
    ]
    cr = Crossref()

    ri = ReferenceInfo()
    print(ri.get(refs))
    result = cr.works(ids = ri.get(refs))
    print(len(refs), len(result))
