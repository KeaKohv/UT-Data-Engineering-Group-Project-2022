// general-structure
match (i:Institution)-[:AFFILIATES]-(a:Author)-[:AUTHORS]->(p:Piece)<-[:PUBLISHES]-(v:Venue) return *


//setup-institution-pagerank
CALL gds.graph.drop('institution-pagerank', false) YIELD graphName;
CALL gds.graph.project.cypher(
  'institution-pagerank',
  'match (i:Institution) return id (i) as id',
  'match (i:Institution)-[:AFFILIATION]-(:Author)-[:AUTHORSHIP]-(:Piece)-[:REFERENCES*1..10]->(:Piece)-[:AUTHORSHIP]-(:Author)-[:AFFILIATION]-(i2:Institution) return id(i) as source, id(i2) as target'
);
// show-institution-pagerank
CALL gds.pageRank.stream('institution-pagerank')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name as name, score
ORDER BY score DESC, name ASC;
//////////////////////////////////////////////////

// community
CALL gds.graph.drop('community', false) YIELD graphName;
CALL gds.graph.project(
    'community',
    ['Author', 'Piece'],
    '*'
);
CALL gds.louvain.stream('community')
YIELD nodeId, communityId, intermediateCommunityIds
WHERE gds.util.asNode(nodeId).family is not null
RETURN gds.util.asNode(nodeId).family AS name, communityId
ORDER BY communityId ASC
//////////////////////////////////////////////////

//setup-reference-pagerank
CALL gds.graph.drop('piece-references-pagerank', false) YIELD graphName;
CALL gds.graph.project(
  'piece-references-pagerank',
  'Piece',
  'REFERENCES'
);
//show-reference-pagerank
CALL gds.pageRank.stream('piece-references-pagerank')
YIELD nodeId, score
WHERE gds.util.asNode(nodeId).year is not NULL 
RETURN gds.util.asNode(nodeId).title AS title, gds.util.asNode(nodeId).year as year, score
ORDER BY score DESC, year, title ASC;
//////////////////////////////////////////////////

//setup-author-articlerank
CALL gds.graph.drop('author-articlerank', false) YIELD graphName;
CALL gds.graph.project.cypher(
  'author-articlerank',
  'match (a:Author) return id (a) as id',
  'match (a:Author)-[:AUTHORSHIP]-(:Piece)-[:REFERENCES*1..10]->(:Piece)-[:AUTHORSHIP]-(a2:Author) return id(a) as source, id(a2) as target'
);// show-author-articlerank
CALL gds.pageRank.stream('author-articlerank')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).given as given, gds.util.asNode(nodeId).family AS family, score
ORDER BY score DESC, family ASC;
//////////////////////////////////////////////////

//setup-venue-articlerank
CALL gds.graph.drop('venue-articlerank', false) YIELD graphName;
CALL gds.graph.project.cypher(
  'venue-articlerank',
  'match (a:Venue) return id (a) as id',
  'match (v1:Venue)-[:PUBLICATION]-(:Piece)-[:REFERENCES*1..10]->(:Piece)-[:PUBLICATION]-(v2:Venue) return id(v2) as source, id(v2) as target'
);
// show-venue-articlerank
CALL gds.pageRank.stream('venue-articlerank')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).title as title, score
ORDER BY score DESC, title ASC;
//////////////////////////////////////////////////
CALL gds.graph.drop('article-centrality', false) YIELD graphName;
CALL gds.graph.project('article-centrality', 
['Author', 'Piece'],
['AUTHORSHIP', 'REFERENCES']
);
CALL gds.betweenness.stream('article-centrality')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).title AS title, score
ORDER BY score DESC, title ASC



