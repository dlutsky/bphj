#ifndef SEMANTIC_ANALYZER_H
#define SEMANTIC_ANALYZER_H

#include <cstdint>
#include <cstddef>
#include <memory>
#include <vector>
#include <unordered_map>
#include "query_graph.h"
#include "storage/dictionary.h"


class SemanticAnalyzer {
public:
  SemanticAnalyzer(Dictionary& dict);
  ~SemanticAnalyzer();

  void analyse(QueryGraph* query_graph);

  static void analyse(QueryGraph* query_graph, Dictionary& dict);

private:
  void analyseProjection();
  void analysePatternGroup(QueryPattern* pattern);
  void analyseBasicGraphPattern(QueryPattern* pattern);
  void encodeNode(QueryNode* node);
  void encodeResource(QueryResource* res);


  Dictionary& dict;
  QueryGraph* query_graph;
};


#endif
