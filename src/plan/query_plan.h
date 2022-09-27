#ifndef QUERY_PLAN_H
#define QUERY_PLAN_H

#include <cstdint>
#include <vector>
#include <set>
#include <map>
#include "common/triple.h"
#include "query/query_graph.h"
#include "runtime/runtime.h"
#include "util/bit_set.h"

/*
#ifndef TYPEDEF_CARD_T
#define TYPEDEF_CARD_T
typedef double card_t;
#endif

#ifndef TYPEDEF_COST_T
#define TYPEDEF_COST_T
typedef double cost_t;
#endif
*/


struct JoinNode {
  uint32_t left_join_key;
  uint32_t right_join_key;
  ResourcePosition left_res_pos;
  ResourcePosition right_res_pos;
  bool can_filter;
  JoinNode(uint32_t left_join_key, uint32_t right_join_key) : left_join_key(left_join_key), right_join_key(right_join_key), can_filter(false) {}
};

struct PlanNode {
  enum Operator { TableScan, Filter, HashJoin, BackProbeHashJoin, ResultsPrinter };
  Operator op;

  PlanNode* next;

  std::vector<PlanNode*> child_nodes;
  std::set<uint32_t> required_res;
  BitSet available_res;

  //Store for BackProbeHashJoin
  std::vector<JoinNode> join_nodes;
  BitSet head, tail;
  bool is_star;
  std::vector<uint32_t> join_res;

  //Store for TableScan
  int node_index;
  TripleOrder triple_order;
  QueryNode query_node;

  // Store for Filter
  uint32_t filter_key;
  uint32_t filter_value;

  // Store for ResultsPrinter
  std::vector<QueryResource> projection;

  double cardinality;
  double costs;
  std::map<uint32_t, double> densities;

  int ordering;
};


class QueryPlanner;

class QueryPlan {
public:
  QueryPlan();
  ~QueryPlan();

  void execute(Runtime& runtime, bool silent);
  void print();

protected:
  PlanNode* createPlanNode();
  void freePlanNode(PlanNode* node);
  void print(PlanNode* node, int deep);

  friend class QueryPlanner;

  PlanNode* root;
  MemoryPool<PlanNode> node_pool;
};


#endif
