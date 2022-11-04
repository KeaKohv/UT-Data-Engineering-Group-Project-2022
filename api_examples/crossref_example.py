from habanero import Crossref
# https://api.crossref.org/swagger-ui/index.html#/Works/get_works
cr = Crossref()
response = cr.works(
    query={'author': 'P. M. Nadolsky'}
)
print(response)