#include "query_graph.h"


QueryNode::QueryNode(QueryResource subject, QueryResource predicate, QueryResource object)
  : subject(subject), predicate(predicate), object(object) {
}

QueryGraph::QueryGraph() : var_count(0) {}

QueryGraph::~QueryGraph() {}

// get

uint32_t QueryGraph::getVariableId(const std::string& value) {
  if(!var_id_map.count(value)) {
    var_id_map[value] = var_count;
    return var_count++;
  }
  else{
    return var_id_map[value];
  }
}

int QueryGraph::numOfVariables() const {
  return var_count;
}

int QueryGraph::numOfNodes() const {
  return nodes.size();
}

QueryNode& QueryGraph::getNode(int index) {
  return nodes[index];
}

const QueryNode& QueryGraph::getNode(int index) const {
  return nodes[index];
}

QueryForm QueryGraph::getQueryForm() const {
  return query_form;
}

const std::vector<QueryResource>& QueryGraph::getProjection() const {
  return projection;
}

const QueryPattern* QueryGraph::getQueryPattern() const {
  return pattern;
}

DuplicateModifier QueryGraph::getDuplicateModifier() const {
  return duplicate_modifier;
}

int QueryGraph::getLimit() const {
  return limit;
}

int QueryGraph::getOffset() const {
  return offset;
}

// set

int QueryGraph::addNode(const QueryNode& node) {
  int index = nodes.size();
  nodes.push_back(node);
  return index;
}

void QueryGraph::setQueryForm(QueryForm query_form) {
  this->query_form = query_form;
}

void QueryGraph::addProjection(const std::string& name) {
  projection.push_back(QueryResource(QueryResource::Variable, name));
}


void QueryGraph::setDuplicateModifier(const DuplicateModifier& duplicate_modifier) {
  this->duplicate_modifier = duplicate_modifier;
}

void QueryGraph::setLimit(int limit) {
  this->limit = limit;
}

void QueryGraph::setOffset(int offset) {
  this->offset = offset;
}

void QueryGraph::addOrder(const Order& order) {
  this->order.push_back(order);
}

// new
QueryExpression* QueryGraph::createExpression() {
  return expression_pool.alloc();
}

void QueryGraph::removeExpression(QueryExpression* expression) {
  expression_pool.free(expression);
}

QueryPattern* QueryGraph::createPattern() {
  return pattern_pool.alloc();
}

void QueryGraph::removePattern(QueryPattern* pattern) {
  pattern_pool.free(pattern);
}
