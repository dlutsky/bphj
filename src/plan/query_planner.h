#ifndef QUERY_PLANNER_H
#define QUERY_PLANNER_H

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <memory>
#include <climits>
#include <cstring>
#include <algorithm>
#include "query_plan.h"
#include "query/query_graph.h"
#include "database/statistics_manager.h"
#include "util/memory_pool.h"
#include "util/bit_set.h"


class QueryPlanner {
private:
  struct QuerySolution {
    PlanNode* root;
    BitSet nodes;
    QuerySolution(size_t num_of_nodes);
  };

public:
  QueryPlanner(StatisticsManager& stat_manager);
  ~QueryPlanner();

  QueryPlan* build(const QueryGraph& graph);

  static std::unique_ptr<QueryPlan> build(const QueryGraph& graph, StatisticsManager& stat_manager);

private:
  QuerySolution* buildStar(const QueryGraph& graph, std::vector<QuerySolution*>& child_solutions, uint32_t join_key);
  QuerySolution* buildJoin(const QueryGraph& graph, const QueryPattern* pattern, const std::vector<QuerySolution*>& node_solutions);
  QuerySolution* buildUnion(const QueryGraph& graph, const QueryPattern* pattern);
  QuerySolution* buildScan(const QueryGraph& graph, int node_index);

  PlanNode* createHashJoin(const std::vector<PlanNode*>& child_nodes, const std::vector<JoinNode>& join_nodes, const std::vector<uint32_t>& join_res, BitSet& available_res);
  PlanNode* createBackProbeHashJoin(const std::vector<PlanNode*>& child_nodes, const std::vector<JoinNode>& join_nodes, const std::vector<uint32_t>& join_res, BitSet& available_res);
  PlanNode* createTableScanNode(int node_index, const QueryNode& query_node, int total_num_variables);
  PlanNode* createFilterNode(uint32_t filter_key, uint32_t filter_value, PlanNode* child);
  PlanNode* createResultsPrinterNode(const QueryGraph& graph, PlanNode* child);

  void bindResource(PlanNode* node, const std::set<uint32_t>& required_res);

  void optimize(PlanNode* node);
  void optimizeJoin(PlanNode* node);

  StatisticsManager& stat_manager;
  QueryPlan *plan;

  MemoryPool<QuerySolution> solution_pool;
};


#endif
