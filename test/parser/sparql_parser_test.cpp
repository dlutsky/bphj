#include <string>
#include <gtest/gtest.h>
#include <iostream>
#include "query/query_graph.h"
#include "parser/sparql_parser.h"


class SPARQParserTest : public testing::Test {
protected:
};


TEST_F(SPARQParserTest, parse1) {
  std::string query_string = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n";
  query_string += "PREFIX dc:   <http://purl.org/dc/elements/1.1/> \n";
  query_string += "PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#> \n";
  query_string += "SELECT ?name \n";
  query_string += "WHERE { ?x foaf:givenName  ?givenName. }";
  QueryGraph query_graph;
  bool valid = SPARQLParser::parse(query_string, &query_graph);
  EXPECT_TRUE(valid);
  EXPECT_EQ(SelectQuery, query_graph.getQueryForm());
  EXPECT_EQ(1, query_graph.getProjection().size());

  const QueryPattern* graph_pattern = query_graph.getQueryPattern();
  EXPECT_EQ(1, graph_pattern->nodes.size());
  QueryNode& query_node = query_graph.getNode(graph_pattern->nodes[0]);
  EXPECT_EQ(QueryResource::Variable, query_node.subject.type);
  EXPECT_EQ(0, strcmp(query_node.subject.value.c_str(), "?x"));
  EXPECT_EQ(QueryResource::IRI, query_node.predicate.type);
  EXPECT_EQ(0, strcmp(query_node.predicate.value.c_str(), "<http://xmlns.com/foaf/0.1/givenName>"));
  EXPECT_EQ(QueryResource::Variable, query_node.object.type);
  EXPECT_EQ(0, strcmp(query_node.object.value.c_str(), "?givenName"));
}

TEST_F(SPARQParserTest, parse2) {
  std::string query_string = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n";
  query_string += "PREFIX dc:   <http://purl.org/dc/elements/1.1/> \n";
  query_string += "PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#> \n";
  query_string += "SELECT ?name \n";
  query_string += "WHERE { ?x foaf:givenName  ?name . \n";
  query_string += "OPTIONAL { ?x dc:date ?date. } \n";
  query_string += "FILTER ( bound(?date) ) }";
  QueryGraph query_graph;
  bool valid = SPARQLParser::parse(query_string, &query_graph);
  EXPECT_TRUE(valid);
  EXPECT_EQ(SelectQuery, query_graph.getQueryForm());
  EXPECT_EQ(1, query_graph.getProjection().size());

  const QueryPattern* graph_pattern = query_graph.getQueryPattern();
  EXPECT_EQ(1, graph_pattern->nodes.size());
  QueryNode& query_node = query_graph.getNode(graph_pattern->nodes[0]);
  EXPECT_EQ(QueryResource::Variable, query_node.subject.type);
  EXPECT_EQ(0, strcmp(query_node.subject.value.c_str(), "?x"));
  EXPECT_EQ(QueryResource::IRI, query_node.predicate.type);
  EXPECT_EQ(0, strcmp(query_node.predicate.value.c_str(), "<http://xmlns.com/foaf/0.1/givenName>"));
  EXPECT_EQ(QueryResource::Variable, query_node.object.type);
  EXPECT_EQ(0, strcmp(query_node.object.value.c_str(), "?name"));

  EXPECT_EQ(1, graph_pattern->optionals.size());
  const QueryPattern* optional = graph_pattern->optionals[0];
  EXPECT_EQ(1, optional->nodes.size());
  QueryNode& optional_node = query_graph.getNode(optional->nodes[0]);
  EXPECT_EQ(QueryResource::Variable, optional_node.subject.type);
  EXPECT_EQ(0, strcmp(optional_node.subject.value.c_str(), "?x"));
  EXPECT_EQ(QueryResource::IRI, optional_node.predicate.type);
  EXPECT_EQ(0, strcmp(optional_node.predicate.value.c_str(), "<http://purl.org/dc/elements/1.1/date>"));
  EXPECT_EQ(QueryResource::Variable, optional_node.object.type);
  EXPECT_EQ(0, strcmp(optional_node.object.value.c_str(), "?date"));

  EXPECT_EQ(1, graph_pattern->filters.size());
  const QueryExpression* filter = graph_pattern->filters[0];
  EXPECT_EQ(QueryExpression::Builtin_bound, filter->type);
  EXPECT_EQ(1, filter->arg_list.size());
  EXPECT_EQ(QueryExpression::Variable, filter->arg_list[0]->type);
  EXPECT_EQ(0, strcmp(filter->arg_list[0]->value.c_str(), "?date"));
}

TEST_F(SPARQParserTest, parse3) {
  std::string query_string = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n";
  query_string += "PREFIX dc:   <http://purl.org/dc/elements/1.1/> \n";
  query_string += "PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#> \n";
  query_string += "SELECT ?name ?mbox \n";
  query_string += "WHERE { ?x foaf:givenName  ?name . \n";
  query_string += "OPTIONAL { ?x dc:date ?date. } \n";
  query_string += "OPTIONAL { ?x  foaf:mbox  ?mbox. } \n";
  query_string += "FILTER ( bound(?date) ) \n";
  query_string += "FILTER isIRI(?mbox) }";
  QueryGraph query_graph;
  bool valid = SPARQLParser::parse(query_string, &query_graph);
  EXPECT_TRUE(valid);
  EXPECT_EQ(SelectQuery, query_graph.getQueryForm());
  EXPECT_EQ(2, query_graph.getProjection().size());

  const QueryPattern* graph_pattern = query_graph.getQueryPattern();
  EXPECT_EQ(2, graph_pattern->optionals.size());
  EXPECT_EQ(2, graph_pattern->filters.size());
}

TEST_F(SPARQParserTest, parse4) {
  std::string query_string = "PREFIX a:<http://www.w3.org/2000/10/annotation-ns#> \n";
  query_string += "PREFIX dc:<http://purl.org/dc/elements/1.1/> \n";
  query_string += "PREFIX xsd:<http://www.w3.org/2001/XMLSchema#> \n";
  query_string += "SELECT ?annot \n";
  query_string += "WHERE { ?annot  a:annotates  <http://www.w3.org/TR/rdf-sparql-query/> . \n";
  query_string += "?annot  dc:date      ?date . \n";
  query_string += "FILTER ( ?date > \"2005-01-01T00:00:00Z\"^^xsd:dateTime ) }";
  QueryGraph query_graph;
  bool valid = SPARQLParser::parse(query_string, &query_graph);
  EXPECT_TRUE(valid);
  EXPECT_EQ(SelectQuery, query_graph.getQueryForm());
  EXPECT_EQ(1, query_graph.getProjection().size());

  const QueryPattern* graph_pattern = query_graph.getQueryPattern();
  EXPECT_EQ(2, graph_pattern->nodes.size());
  QueryNode& query_node1 = query_graph.getNode(graph_pattern->nodes[0]);
  EXPECT_EQ(QueryResource::Variable, query_node1.subject.type);
  EXPECT_EQ(0, strcmp(query_node1.subject.value.c_str(), "?annot"));
  EXPECT_EQ(QueryResource::IRI, query_node1.predicate.type);
  EXPECT_EQ(0, strcmp(query_node1.predicate.value.c_str(), "<http://www.w3.org/2000/10/annotation-ns#annotates>"));
  EXPECT_EQ(QueryResource::IRI, query_node1.object.type);
  EXPECT_EQ(0, strcmp(query_node1.object.value.c_str(), "<http://www.w3.org/TR/rdf-sparql-query/>"));

  QueryNode& query_node2 = query_graph.getNode(graph_pattern->nodes[1]);
  EXPECT_EQ(QueryResource::Variable, query_node2.subject.type);
  EXPECT_EQ(0, strcmp(query_node2.subject.value.c_str(), "?annot"));
  EXPECT_EQ(QueryResource::IRI, query_node2.predicate.type);
  EXPECT_EQ(0, strcmp(query_node2.predicate.value.c_str(), "<http://purl.org/dc/elements/1.1/date>"));
  EXPECT_EQ(QueryResource::Variable, query_node2.object.type);
  EXPECT_EQ(0, strcmp(query_node2.object.value.c_str(), "?date"));

  const QueryExpression* filter = graph_pattern->filters[0];
  EXPECT_EQ(QueryExpression::Greater, filter->type);
  EXPECT_EQ(2, filter->arg_list.size());
  EXPECT_EQ(QueryExpression::Variable, filter->arg_list[0]->type);
  EXPECT_EQ(0, strcmp(filter->arg_list[0]->value.c_str(), "?date"));
  EXPECT_EQ(QueryExpression::Literal, filter->arg_list[1]->type);
  EXPECT_EQ(0, strcmp(filter->arg_list[1]->value.c_str(), "\"2005-01-01T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));
}

/*
TEST_F(SPARQParserTest, parse5) {
  std::string query_string = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n";
  query_string += "SELECT ?name \n";
  query_string += "WHERE { [ foaf:name  ?name ; \n";
  query_string += "foaf:mbox  <mailto:alice@example.org> ] . }";
  QueryGraph query_graph;
  bool valid = SPARQLParser::parse(query_string, &query_graph);
  EXPECT_TRUE(valid);
  EXPECT_EQ(SelectQuery, query_graph.getQueryForm());
  EXPECT_EQ(1, query_graph.getProjection().size());

  const QueryPattern* graph_pattern = query_graph.getQueryPattern();
  EXPECT_EQ(2, graph_pattern->nodes.size());
  QueryNode& query_node = query_graph.getNode(graph_pattern->nodes[0]);
  EXPECT_EQ(QueryResource::Variable, query_node.subject.type);
  EXPECT_EQ(0, strcmp(query_node.subject.value.c_str(), "_:0"));
  EXPECT_EQ(QueryResource::IRI, query_node.predicate.type);
  EXPECT_EQ(0, strcmp(query_node.predicate.value.c_str(), "<http://xmlns.com/foaf/0.1/name>"));
  EXPECT_EQ(QueryResource::Variable, query_node.object.type);
  EXPECT_EQ(0, strcmp(query_node.object.value.c_str(), "?name"));
}
*/

TEST_F(SPARQParserTest, parse6) {
  std::string query_string = "";
  QueryGraph query_graph;
  bool valid = SPARQLParser::parse(query_string, &query_graph);
  EXPECT_FALSE(valid);
}

TEST_F(SPARQParserTest, parse7) {
  std::string query_string = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n";
  query_string += "PREFIX dc:   <http://purl.org/dc/elements/1.1/> \n";
  query_string += "PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#> \n";
  query_string += "SELECT ?name \n";
  query_string += "WHERE { ?x aaa  ?givenName. }";
  QueryGraph query_graph;
  bool valid = SPARQLParser::parse(query_string, &query_graph);
  EXPECT_FALSE(valid);
}

TEST_F(SPARQParserTest, parse8) {
  SPARQLFileParser parser("test/data/rq_test1.rq");
  QueryGraph query_graph1;
  bool valid1 = parser.parse(&query_graph1);
  EXPECT_TRUE(valid1);
  QueryGraph query_graph2;
  bool valid2 = parser.parse(&query_graph2);
  EXPECT_FALSE(valid2);
}

TEST_F(SPARQParserTest, parse9) {
  SPARQLFileParser parser("test/data/rq_test2.rq");
  QueryGraph query_graph1;
  bool valid1 = parser.parse(&query_graph1);
  EXPECT_TRUE(valid1);
  QueryGraph query_graph2;
  bool valid2 = parser.parse(&query_graph2);
  EXPECT_TRUE(valid2);
  QueryGraph query_graph3;
  bool valid3 = parser.parse(&query_graph3);
  EXPECT_TRUE(valid3);
  QueryGraph query_graph4;
  bool valid4 = parser.parse(&query_graph4);
  EXPECT_FALSE(valid4);
}
