import requests
from more_itertools import chunked

def clean_title(title: str):
    return ''.join(
        c if c.isalnum()
        else ' '
        for c in title
    )

def search_by_title(title, doi=None):
    if doi is not None:
        return requests.get('https://api.openalex.org/works', {"filter":f"doi:https://doi.org/{doi}"})

    return requests.get('https://api.openalex.org/works', {"filter":f"title.search:{title}"})

def search_by_doi(doi):
    return requests.get(f'https://api.openalex.org/works/{doi}')

def search_by_alex_ids(work_alex_ids):
    work_alex_ids = [w.split('/')[-1] for w in work_alex_ids]
    queries = [
        '|'.join(ids)
        for ids in chunked(work_alex_ids, 50)
    ]

    responses = None
    for q in queries:
        response = requests.get('https://api.openalex.org/works', {"filter":f"ids.openalex:{q}"})
        if responses is None:
            responses = response.json()
        else:
            response = response.json()
            responses["results"] += response["results"]

    if responses is None:
        return []
    return responses["results"]

def search_by_orcid(orcid):
    # https: // api.openalex.org / authors / A2208157607
    return requests.get(f'https://api.openalex.org/authors/{orcid}')

def search_by_orcids(orcids):
    queries = [
        '|'.join(ids)
        for ids in chunked(orcids, 50)
    ]

    responses = None
    for q in queries:
        response = requests.get('https://api.openalex.org/authors', {"filter":f"ids.orcid:{q}"})
        if responses is None:
            responses = response.json()
        else:
            responses["results"] += response["results"]
    return responses["results"]

def get_references(work):
    refs = search_by_alex_ids(work["referenced_works"])
    return refs

def get_author(authorship):
    display_name = authorship["author"]["display_name"]
    if ',' in display_name:
        family, *given = display_name.split(',')
        given = ' '.join(g.strip() for g in given)
    else:
        *given, family = display_name.split(' ')
        given = ' '.join(g.strip() for g in given)

    if '.' in family and ',' in ' '.join(given):
        middle = family
        family, given = ' '.join(given).split(',')
        given = ' '.join([given.strip(), middle.strip()])

    if len(authorship["institutions"]):
        affiliation = authorship["institutions"][0]["display_name"].strip()
    else:
        affiliation = []

    return dict(family=family, given=given, affiliation=[dict(name=affiliation)])


class OpenAlexFieldExtractor:
    fields = ['subject', 'published', 'reference']

    def __call__(self, work, references=True):
        published_year, published_month, _ = work["publication_date"].split('-')
        title = work.get("title")
        doi = work.get("doi")
        type = work.get("type")
        publisher = work.get("host_venue").get("publisher")
        container_title = work.get("host_venue").get("display_name")
        author = [get_author(a) for a in work.get("authorships", [])]
        is_referenced_by_count = work.get("cited_by_count")
        level_1_concepts = [c for c in work["concepts"] if c["level"] == 1]
        if len(level_1_concepts) == 0:
            level_1_concepts = [c for c in work["concepts"] if c["level"] == 0]
        else:
            subject = max(level_1_concepts, key=lambda x: x["score"])["display_name"]
        if len(level_1_concepts) == 0:
            subject = None
        else:
            subject = max(level_1_concepts, key=lambda x: x["score"])["display_name"]

        if references:
            reference = [
                self.__call__(w, references=False)
                for w in get_references(work)
            ]
        else:
            reference = []

        return {
            'published-year':published_year,
            'published-month':published_month,
            'title':title,
            'doi':doi,
            'type':type,
            'publisher':publisher,
            'container-title':container_title,
            'author':author,
            'is-referenced-by-count':is_referenced_by_count,
            'subject':subject,
            'reference': reference,
        }


def process_openalex_work(authors=None, doi=None, title=None):
    extract = OpenAlexFieldExtractor()
    title = clean_title(title)
    print('searching for:', title)
    response = search_by_title(title, doi=doi)
    response = response.json()
    candidates = list(filter(lambda x: x["doi"] is not None, response["results"]))
    if len(candidates) > 0:
        result = candidates[-1]
        work = extract(result)
        print('matched: ', result["title"])
        print('old authors:', authors)
        print('new authors:', [a["family"] for a in work['author']])
        print(work)
        print()
        return work
    else:
        print("Failed to get result for:")
        print("* title", title)
        print("* authors", authors)
        print("* doi", doi)
        print()
        return {}

def main():
    response = search_by_title("Calculation of prompt diphoton production cross sections at Fermilab Tevatron and CERN LHC energies")
    response = response.json()
    if len(response)["results"] > 0:
        work = response["results"]
        refs = get_references(work)

        for r in refs:
            print([get_author(a) for a in r["authorships"]])



    # for authorship in response.json()["results"][0]['authorships']:
    #     author_orcid = authorship["author"]["orcid"]
    #     response = search_by_orcid(author_orcid)
    #     json = response.json()
    #     *given, family = json["display_name"].split(' ')
    #     print(family, ' '.join(given))
    #     print(response.content)


    # doi = 'https://doi.org/10.1103/PhysRevD.76.013009'
    # response = search_by_doi(doi)

if __name__ == '__main__':
    main()
