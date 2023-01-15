--- QUERIES

--- ranking authors in a scientific domain by the total number of papers
SELECT DENSE_RANK() OVER(ORDER BY x.count DESC) ranking, x.full_name
FROM (
	SELECT a.full_name, COUNT(p.title) OVER(PARTITION BY a.full_name) count
	FROM dim_author a
	JOIN bridge_author_group aug ON a.author_key=aug.author_key
	JOIN paper_fact p ON aug.author_group_key=p.author_group_key
	JOIN dim_domain d ON p.domain_key=d.domain_key
	WHERE scientific_domain LIKE '%Physics%') x;
		
--- ranking authors in a scientific domain by the total number of citations of their papers
SELECT DENSE_RANK() OVER(ORDER BY x.sum DESC) ranking, x.full_name
FROM (
	SELECT DISTINCT a.full_name, SUM(p.citation_count) OVER(PARTITION BY a.author_key) sum
	FROM dim_author a
	JOIN bridge_author_group aug ON a.author_key=aug.author_key
	JOIN paper_fact p ON aug.author_group_key=p.author_group_key
	JOIN dim_domain d ON p.domain_key=d.domain_key
	WHERE scientific_domain LIKE '%Physics%') x;
			
--- ranking authors in a scientific domain by H-index
SELECT DENSE_RANK() OVER(ORDER BY x.h_index DESC) ranking, x.h_index, x.full_name, x.scientific_domain
FROM (
	SELECT a.full_name, a.h_index, d.scientific_domain
	FROM dim_author a
	JOIN bridge_author_group aug ON a.author_key=aug.author_key
	JOIN paper_fact p ON aug.author_group_key=p.author_group_key
	JOIN dim_domain d ON p.domain_key=d.domain_key
	WHERE scientific_domain LIKE '%Physics%') x;

--- ranking authors in a scientific domain by G-index
SELECT DENSE_RANK() OVER(ORDER BY x.g_index DESC) ranking, x.g_index, x.full_name, x.scientific_domain
FROM (
	SELECT a.full_name, a.g_index, d.scientific_domain
	FROM dim_author a
	JOIN bridge_author_group aug ON a.author_key=aug.author_key
	JOIN paper_fact p ON aug.author_group_key=p.author_group_key
	JOIN dim_domain d ON p.domain_key=d.domain_key
	WHERE scientific_domain LIKE '%Physics%') x;
	
--- ranking papers by citation count
SELECT DENSE_RANK() OVER(ORDER BY citation_count DESC) ranking, citation_count, title
FROM paper_fact;

--- ranking affiliations by total number of papers from affiliated authors
SELECT DENSE_RANK() OVER(ORDER BY x.count DESC) ranking, x.affiliation_name
FROM (
	SELECT af.affiliation_name, COUNT(p.title) OVER(PARTITION BY af.affiliation_key) count
	FROM dim_affiliation af
	JOIN bridge_affiliation_group afg ON af.affiliation_key=afg.affiliation_key
	JOIN paper_fact p ON afg.affiliation_group_key=p.affiliation_group_key) x
	WHERE x.affiliation_name != 'Unknown';
	
--- ranking affiliations by number of papers published this century by affiliated authors
SELECT DENSE_RANK() OVER(ORDER BY x.count DESC) ranking, x.affiliation_name
FROM (
	SELECT af.affiliation_name, COUNT(p.title) OVER(PARTITION BY af.affiliation_key) count
	FROM dim_affiliation af
	JOIN bridge_affiliation_group afg ON af.affiliation_key=afg.affiliation_key
	JOIN paper_fact p ON afg.affiliation_group_key=p.affiliation_group_key
	JOIN dim_year y ON p.year_key=y.year_key
	WHERE y.publication_year BETWEEN 2000 AND 2022
	AND af.affiliation_name != 'Unknown') x;

--- ranking affiliations by total number of paper citations from affiliated authors
SELECT DENSE_RANK() OVER(ORDER BY x.sum DESC) ranking, x.sum, x.affiliation_name
FROM (
	SELECT af.affiliation_name, SUM(p.citation_count) OVER(PARTITION BY af.affiliation_key) sum
	FROM dim_affiliation af
	JOIN bridge_affiliation_group afg ON af.affiliation_key=afg.affiliation_key
	JOIN paper_fact p ON afg.affiliation_group_key=p.affiliation_group_key
    WHERE af.affiliation_name != 'Unknown') x;
	
--- ranking affiliations by the average number of citations per author
SELECT DENSE_RANK() OVER(ORDER BY x.average DESC) ranking, x.affiliation_name, x.average
FROM (
	SELECT af.affiliation_name, ROUND(AVG(p.citation_count) OVER(PARTITION BY af.affiliation_name)) average
	FROM dim_affiliation af
	JOIN bridge_affiliation_group afg ON af.affiliation_key=afg.affiliation_key
	JOIN paper_fact p ON afg.affiliation_group_key=p.affiliation_group_key
    WHERE af.affiliation_name != 'Unknown') x;

--- ranking publication venues by total number of published papers
SELECT DENSE_RANK() OVER(ORDER BY x.count DESC) ranking, x.count, x.pub_venue, x.publisher
FROM (
	SELECT DISTINCT v.pub_venue, v.publisher, COUNT(p.title) OVER(PARTITION BY v.pub_venue) count
	FROM dim_venue v
	JOIN paper_fact p ON v.venue_key=p.venue_key) x;
	
--- ranking publication venues by total number of paper citations
SELECT DENSE_RANK() OVER(ORDER BY x.sum DESC) ranking, x.sum, x.pub_venue, x.publisher
FROM (
	SELECT DISTINCT v.pub_venue,  v.publisher, SUM(p.citation_count) OVER(PARTITION BY v.pub_venue) sum
	FROM dim_venue v
	JOIN paper_fact p ON v.venue_key=p.venue_key) x;

--- ranking publication venues by the average number of citations per paper
SELECT DENSE_RANK() OVER(ORDER BY x.average DESC) ranking, x.pub_venue
FROM (
	SELECT DISTINCT v.pub_venue, ROUND(AVG(p.citation_count) OVER(PARTITION BY v.pub_venue)) average
	FROM dim_venue v
	JOIN paper_fact p ON v.venue_key=p.venue_key) x;

--- ranking publication venues by the number of published math papers to see what are the top math publication venues
SELECT DENSE_RANK() OVER(ORDER BY x.count DESC) ranking, x.count, x.pub_venue
FROM (
	SELECT DISTINCT v.pub_venue, COUNT(p.title) OVER(PARTITION BY v.pub_venue) count
	FROM dim_venue v
	JOIN paper_fact p ON v.venue_key=p.venue_key
	JOIN dim_domain d ON p.domain_key=d.domain_key
	WHERE d.scientific_domain LIKE '%math%') x;
	
--- finding years with most published papers
SELECT DENSE_RANK() OVER(ORDER BY x.count DESC) ranking, x.count, x.publication_year
FROM (
	SELECT DISTINCT y.publication_year, COUNT(p.title) OVER(PARTITION BY y.publication_year) count
	FROM dim_year y
	JOIN paper_fact p ON y.year_key=p.year_key) x;

--- histograms the number of papers on a given scientific domain over a given time period (years)
SELECT DISTINCT y.publication_year, COUNT(p.title) OVER(PARTITION BY y.publication_year) count_papers
FROM dim_year y
JOIN paper_fact p ON y.year_key=p.year_key
JOIN dim_domain d ON p.domain_key=d.domain_key
WHERE d.scientific_domain LIKE '%math%' AND
y.publication_year BETWEEN 2010 AND 2022
ORDER BY y.publication_year ASC;