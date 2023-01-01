DROP TABLE IF EXISTS staging_main;
DROP TABLE IF EXISTS staging_authors;
DROP TABLE IF EXISTS staging_affiliations;

CREATE TABLE staging_main (
    staging_main_key INT GENERATED ALWAYS AS IDENTITY UNIQUE,

    publication_year INT NOT NULL,
    publication_year_key INT,

    scientific_domain VARCHAR(50) NOT NULL,
    scientific_domain_key INT,

    type_name VARCHAR(30) NOT NULL,
    type_key INT,

    pub_venue VARCHAR(100) NOT NULL,
    publisher VARCHAR(200) NOT NULL,
    venue_key INT,

    arxiv_ID VARCHAR(9) NOT NULL UNIQUE, -- NK
    doi VARCHAR(40) NOT NULL UNIQUE,
    title VARCHAR(400) NOT NULL,
    latest_version_nr VARCHAR(3),
    citaton_count INT,

    PRIMARY KEY (staging_main_key)
);

CREATE TABLE staging_authors (
    staging_author_key INT GENERATED ALWAYS AS IDENTITY,
    arxiv_ID VARCHAR(9) NOT NULL,
    author_key INT,
    author_group_key INT,
    full_name VARCHAR(100) NOT NULL,
    gender VARCHAR(10) NOT NULL,
    h_index INT,
    g_index INT,
    PRIMARY KEY (staging_author_key)
);

CREATE TABLE staging_affiliations (
    staging_affiliation_key INT GENERATED ALWAYS AS IDENTITY,
    arxiv_ID VARCHAR(9) NOT NULL,
    affiliation_key INT,
    affiliation_group_key INT,
    affiliation_name VARCHAR(400) NOT NULL,
    PRIMARY KEY (staging_affiliation_key)
);