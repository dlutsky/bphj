#include "semantic_analyzer.h"


SemanticAnalyzer::SemanticAnalyzer(Dictionary& dict) : dict(dict) {
}

SemanticAnalyzer::~SemanticAnalyzer() {
}

void SemanticAnalyzer::analyseProjection() {
  for(std::vector<QueryResource>::iterator it = query_graph->projection.begin(), end = query_graph->projection.end(); it != end; ++it) {
    it->id = query_graph->getVariableId(it->value);
  }
}

void SemanticAnalyzer::analysePatternGroup(QueryPattern* pattern) {
  switch(pattern->type) {
    case QueryPattern::Basic:
      analyseBasicGraphPattern(pattern);
      break;
    case QueryPattern::Join: {
      analyseBasicGraphPattern(pattern);
      for(std::vector<QueryPattern*>::iterator it = pattern->sub_patterns.begin(); it != pattern->sub_patterns.end(); ++it){
        analysePatternGroup(*it);
      }
      break;
    }
    case QueryPattern::Union: {
      for(std::vector<QueryPattern*>::iterator it = pattern->sub_patterns.begin(); it != pattern->sub_patterns.end(); ++it){
        analysePatternGroup(*it);
      }
      break;
    }
  }
}

void SemanticAnalyzer::analyseBasicGraphPattern(QueryPattern* pattern) {
  for(std::vector<int>::const_iterator it = pattern->nodes.begin(); it != pattern->nodes.end(); ++it){
    QueryNode& node = this->query_graph->nodes[*it];
    encodeNode(&node);
  }
  for(std::vector<QueryPattern*>::iterator it = pattern->optionals.begin(); it != pattern->optionals.end(); ++it){
    analysePatternGroup(*it);
  }
}

void SemanticAnalyzer::encodeNode(QueryNode* node) {
  encodeResource(&node->subject);
  encodeResource(&node->predicate);
  encodeResource(&node->object);
}


void SemanticAnalyzer::encodeResource(QueryResource* res) {
  switch(res->type){
    case QueryResource::Variable:
      res->id = query_graph->getVariableId(res->value);
      break;
    case QueryResource::Literal:
    case QueryResource::IRI:
      uint32_t code;
      dict.lookup(res->value, &code);
      res->id = code;
      break;
  }
}

void SemanticAnalyzer::analyse(QueryGraph* query_graph) {
  this->query_graph = query_graph;
  if(query_graph->getQueryForm() == QueryForm::SelectQuery) {
    analyseProjection();
  }
  analysePatternGroup(query_graph->pattern);
}

void SemanticAnalyzer::analyse(QueryGraph* query_graph, Dictionary& dict) {
  SemanticAnalyzer analyzer(dict);
  analyzer.analyse(query_graph);
}
