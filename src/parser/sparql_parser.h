#ifndef SPARQL_PARSER_H
#define SPARQL_PARSER_H

#include <cstdint>
#include <string>
#include "query/query_graph.h"



struct SPARQLParseState;


class SPARQLParser {
public:
  SPARQLParser(const std::string& query_string);
  ~SPARQLParser();

  bool parse(QueryGraph* query_graph);
  static bool parse(const std::string& query_string, QueryGraph* query_graph);

protected:
  SPARQLParser(FILE * input_file);
  SPARQLParseState* state;
  FILE * input_file;
};

class SPARQLFileParser : public SPARQLParser {
public:
  SPARQLFileParser(const std::string& file_path);
  ~SPARQLFileParser();
};





#endif
