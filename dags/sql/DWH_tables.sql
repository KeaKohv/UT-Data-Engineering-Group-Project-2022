DROP TABLE IF EXISTS bridge_author_group;
DROP TABLE IF EXISTS bridge_affiliation_group;
DROP TABLE IF EXISTS paper_fact;
DROP TABLE IF EXISTS dim_type;
DROP TABLE IF EXISTS dim_year;
DROP TABLE IF EXISTS dim_domain;
DROP TABLE IF EXISTS dim_venue;
DROP TABLE IF EXISTS dim_author;
DROP TABLE IF EXISTS dim_affiliation;

CREATE TABLE dim_year (
    year_key INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    publication_year INT NOT NULL UNIQUE,
    PRIMARY KEY (year_key)
);

CREATE TABLE dim_domain (
    domain_key INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    scientific_domain VARCHAR(200) NOT NULL UNIQUE,
    PRIMARY KEY (domain_key)
);

CREATE TABLE dim_type (
    type_key INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    type_name VARCHAR(100) NOT NULL UNIQUE,
    PRIMARY KEY (type_key)
);

CREATE TABLE dim_venue (
    venue_key INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    pub_venue VARCHAR(300) NOT NULL UNIQUE,
    publisher VARCHAR(300) NOT NULL,
    UNIQUE(pub_venue, publisher),
    PRIMARY KEY (venue_key)
);

CREATE TABLE dim_author (
    author_key INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    full_name VARCHAR(300) NOT NULL,
    gender VARCHAR(50) NOT NULL,
    h_index INT NOT NULL DEFAULT 0,
    g_index INT  NOT NULL DEFAULT 0,
    PRIMARY KEY (author_key)
);

CREATE TABLE dim_affiliation (
    affiliation_key INT GENERATED ALWAYS AS IDENTITY UNIQUE,
    affiliation_name VARCHAR(1000) NOT NULL,
    PRIMARY KEY (affiliation_key)
);

CREATE TABLE paper_fact (
    year_key INT NOT NULL,
    domain_key INT NOT NULL,
    type_key INT NOT NULL,
    venue_key INT NOT NULL,
    author_group_key SERIAL UNIQUE,
    affiliation_group_key SERIAL UNIQUE,
    arxiv_ID VARCHAR(100) NOT NULL UNIQUE, -- NK
    doi VARCHAR(100) NOT NULL UNIQUE,
    title VARCHAR(1000) NOT NULL,
    latest_version_nr VARCHAR(50),
    citation_count INT,
    CONSTRAINT uq_fact_table UNIQUE(year_key, domain_key, type_key, venue_key, author_group_key, affiliation_group_key),
    PRIMARY KEY(year_key, domain_key, type_key, venue_key, author_group_key, affiliation_group_key) 
);

CREATE TABLE bridge_author_group (
    author_group_key INT NOT NULL,
    author_key INT NOT NULL,
    CONSTRAINT fk_author_group_author_bridge FOREIGN KEY(author_group_key) REFERENCES paper_fact(author_group_key),
    CONSTRAINT fk_author_key_author_bridge FOREIGN KEY(author_key) REFERENCES dim_author(author_key),
    CONSTRAINT uq_bridge_author_group UNIQUE(author_group_key, author_key),
    PRIMARY KEY (author_group_key, author_key)
);

CREATE TABLE bridge_affiliation_group (
    affiliation_group_key INT NOT NULL,
    affiliation_key INT NOT NULL,
    CONSTRAINT fk_affiliation_group_affiliation_bridge FOREIGN KEY(affiliation_group_key) REFERENCES paper_fact(affiliation_group_key),
    CONSTRAINT fk_affiliation_key_affiliation_bridge FOREIGN KEY(affiliation_key) REFERENCES dim_affiliation(affiliation_key),
    CONSTRAINT uq_bridge_affiliation_group UNIQUE(affiliation_group_key, affiliation_key),
    PRIMARY KEY (affiliation_group_key, affiliation_key)
);

-- year dimension data
INSERT INTO dim_year(publication_year) VALUES (generate_series(1940,2030));
INSERT INTO dim_year(publication_year) VALUES (0); -- For unknown years