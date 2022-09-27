#ifndef QUERY_GRAPH_H
#define QUERY_GRAPH_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <unordered_map>
#include "util/memory_pool.h"


enum QueryForm { SelectQuery, ConstructQuery, AskQuery, DescribeQuery };

struct QueryResource {
  enum Type { Variable, Literal, IRI };
  Type type;
  uint32_t id;
  std::string value;

  QueryResource(Type type, const std::string& value) : type(type), value(value) {}
};

struct QueryNode {
  QueryResource subject, predicate, object;
  QueryNode(QueryResource subject, QueryResource predicate, QueryResource object);
};

struct QueryExpression {
  enum Type {
       Literal, Variable, IRI, Function, ArgumentList,
       Or, And, Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual,
       Plus, Minus, Mul, Div, UnaryPlus, UnaryMinus, Not,
       Builtin_str, Builtin_lang, Builtin_langmatches, Builtin_datatype, Builtin_bound, Builtin_sameterm,
       Builtin_isiri, Builtin_isuri, Builtin_isblank, Builtin_isliteral, Builtin_regex
  };
  Type type;
  uint32_t id;
  std::string value;
  std::vector<QueryExpression*> arg_list;
};

struct QueryPattern {
  enum Type { Basic, Join, Union };
  Type type;

  std::vector<int> nodes;
  std::vector<QueryExpression*> filters;
  std::vector<QueryPattern*> optionals;
  std::vector<QueryPattern*> sub_patterns;
};

enum DuplicateModifier { None, Distinct, Reduced };

struct Order {
  bool ascending;
  QueryExpression *condition;
};

struct SPARQLParseContext;
struct YYLTYPE;
class SemanticAnalyzer;

class QueryGraph {
public:
  QueryGraph();
  ~QueryGraph();

  QueryForm getQueryForm() const;
  const std::vector<QueryResource>& getProjection() const;
  uint32_t getVariableId(const std::string& value);
  int numOfVariables() const;
  int numOfNodes() const;
  QueryNode& getNode(int index);
  const QueryNode& getNode(int index) const;
  const QueryPattern* getQueryPattern() const;
  DuplicateModifier getDuplicateModifier() const;
  int getLimit() const;
  int getOffset() const;

private:
  void setQueryForm(QueryForm query_form);
  void addProjection(const std::string& name);
  int addNode(const QueryNode& node);
  QueryExpression* createExpression();
  void removeExpression(QueryExpression* expression);
  QueryPattern* createPattern();
  void removePattern(QueryPattern* pattern);
  void setDuplicateModifier(const DuplicateModifier& duplicate_modifier);
  void setLimit(int limit);
  void setOffset(int offset);
  void addOrder(const Order& order);

  friend class SemanticAnalyzer;
  friend int yyparse(void* scanner, SPARQLParseContext* context, QueryGraph* query_graph);

  MemoryPool<QueryExpression> expression_pool;
  MemoryPool<QueryPattern> pattern_pool;

  QueryForm query_form;
  std::vector<QueryResource> projection;
  QueryPattern *construct_pattern;
  QueryPattern* pattern;
  std::vector<QueryNode> nodes;
  DuplicateModifier duplicate_modifier;
  int limit;
  int offset;
  std::vector<Order> order;

  std::unordered_map<std::string, uint32_t> var_id_map;
  uint32_t var_count;
};


#endif
