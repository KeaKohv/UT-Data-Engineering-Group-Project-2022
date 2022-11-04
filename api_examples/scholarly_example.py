from pprint import pprint

from scholarly import scholarly
import json
author_name = [
  "Nadolsky",
  "P. M."
]

# NOTE: removed dots and middle name
fullname = 'P Nadolsky'
print(fullname)
# Retrieve the author's data, fill-in, and print
# Get an iterator for the author results
search_query = scholarly.search_author(fullname)

# Retrieve the first result from the iterator
first_author_result = next(search_query)
pprint(first_author_result)

# Retrieve all the details for the author
author = scholarly.fill(first_author_result )
pprint(first_author_result)

# Take a closer look at the first publication
first_publication = author['publications'][0]
pprint(author['publications'])
first_publication_filled = scholarly.fill(first_publication)
pprint(first_publication_filled)
# scholarly.pprint(first_publication_filled)

# Print the titles of the author's publications
publication_titles = [pub['bib']['title'] for pub in author['publications']]
pprint(publication_titles)

"""
IMPORTANT: Making certain types of queries, such as scholarly.citedby or scholarly.search_pubs, will lead to Google Scholar blocking your requests and may eventually block your IP address.
"""
# Which papers cited that publication?
# citations = [citation['bib']['title'] for citation in scholarly.citedby(first_publication_filled)]
# print(citations)