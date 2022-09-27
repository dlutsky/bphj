#include "query_planner.h"
#include "lb_cost_model.h"
#include "util/math_functions.h"


QueryPlanner::QueryPlanner(StatisticsManager& stat_manager) : stat_manager(stat_manager), plan(nullptr) {}

QueryPlanner::~QueryPlanner() { delete plan; }

QueryPlanner::QuerySolution::QuerySolution(size_t num_of_nodes) : nodes(num_of_nodes) {}

QueryPlan* QueryPlanner::build(const QueryGraph& graph) {
  QueryPlan* new_plan = new QueryPlan();
  plan = new_plan;

  std::vector<QuerySolution*> node_solutions(graph.numOfNodes());
  std::unordered_map<int, std::vector<int>> res_to_node;
  int max_count = 0;
  uint32_t max_res = 0;
  for(int i=0; i<graph.numOfNodes(); i++) {
    QuerySolution* solution = buildScan(graph, i);
    node_solutions[i] = solution;

    if(solution->root->query_node.subject.type == QueryResource::Variable) {
      res_to_node[solution->root->query_node.subject.id].push_back(i);
      if(res_to_node[solution->root->query_node.subject.id].size() > max_count) {
        max_count = res_to_node[solution->root->query_node.subject.id].size();
        max_res = solution->root->query_node.subject.id;
      }
    }
    if(solution->root->query_node.object.type == QueryResource::Variable) {
      res_to_node[solution->root->query_node.object.id].push_back(i);
      if(res_to_node[solution->root->query_node.object.id].size() > max_count) {
        max_count = res_to_node[solution->root->query_node.object.id].size();
        max_res = solution->root->query_node.object.id;
      }
    }
  }

  if(res_to_node[max_res].size() > 2 && (double)res_to_node[max_res].size()/node_solutions.size() > 0.5) {
    std::vector<QuerySolution*> sub_solutions;
    std::vector<int> node_indexes = res_to_node[max_res];
    int i=0, j=0;
    std::vector<QuerySolution*>::iterator iter = node_solutions.begin();
    while(iter != node_solutions.end()) {
      if(i<node_indexes.size() && j==node_indexes[i]) {
        sub_solutions.push_back(*iter);
        iter = node_solutions.erase(iter);
        i++;
      } else {
        ++iter;
      }
      j++;
    }

    node_solutions.push_back(buildStar(graph, sub_solutions, max_res));
  }

  QuerySolution *solution = buildJoin(graph, graph.getQueryPattern(), node_solutions);
  PlanNode* join_node = solution->root;
  PlanNode* best_node = solution->root;
  while(join_node != nullptr) {
    if(join_node->costs < best_node->costs) {
      best_node = join_node;
    }
    join_node = join_node->next;
  }

  plan->root = createResultsPrinterNode(graph, best_node);

  std::set<uint32_t> required_res;
  bindResource(plan->root, required_res);
  optimize(plan->root);

  plan = nullptr;
  return new_plan;
}

std::unique_ptr<QueryPlan> QueryPlanner::build(const QueryGraph& graph, StatisticsManager& stat_manager) {
  QueryPlanner planner(stat_manager);
  return std::unique_ptr<QueryPlan>(planner.build(graph));
}

QueryPlanner::QuerySolution* QueryPlanner::buildStar(const QueryGraph& graph, std::vector<QuerySolution*>& child_solutions, uint32_t join_key) {
  QuerySolution* star_solution = solution_pool.alloc();

  struct {
    bool operator()(QuerySolution* a, QuerySolution* b) const { return a->root->cardinality < b->root->cardinality; }
  } customLess;
  std::sort(child_solutions.begin(), child_solutions.end(), customLess);

  star_solution->nodes = BitSet(graph.numOfNodes());
  std::vector<JoinNode> join_nodes;
  std::vector<uint32_t> join_res;

  BitSet available_res(graph.numOfVariables());
  std::vector<PlanNode*> child_nodes;
  for(int i=0; i<child_solutions.size(); i++) {
    star_solution->nodes |= child_solutions[i]->nodes;
    if(child_solutions[i]->root->query_node.subject.type == QueryResource::Variable) {
      available_res.set(child_solutions[i]->root->query_node.subject.id);
    }
    if(child_solutions[i]->root->query_node.object.type == QueryResource::Variable) {
      available_res.set(child_solutions[i]->root->query_node.object.id);
    }
    child_nodes.push_back(child_solutions[i]->root);
    join_nodes.push_back(JoinNode(UINT32_MAX, join_key));
  }
  join_res.push_back(join_key);

  PlanNode* node = createBackProbeHashJoin(child_nodes, join_nodes, join_res, available_res);

  node->costs = child_nodes[0]->costs + child_nodes[0]->cardinality;
  std::unordered_map<uint32_t, double> costs;
  for(BitSet::SetBitIterator iter = child_nodes[0]->available_res.begin(); iter != child_nodes[0]->available_res.end(); ++iter) {
    costs[*iter] = child_nodes[0]->cardinality*child_nodes[0]->densities[*iter];
  }
  for(int x=1; x<child_nodes.size(); ++x) {
    double cost = child_nodes[x]->cardinality;
    for(BitSet::SetBitIterator iter = child_nodes[x]->available_res.begin(); iter != child_nodes[x]->available_res.end(); ++iter) {
      if(costs.count(*iter)) {
        cost = std::min(cost, costs[*iter]/child_nodes[x]->densities[*iter]);
      }
    }
    node->costs += child_nodes[x]->costs + cost;
    for(BitSet::SetBitIterator iter = child_nodes[x]->available_res.begin(); iter != child_nodes[x]->available_res.end(); ++iter) {
      if(costs.count(*iter)) {
        costs[*iter] = std::min(costs[*iter], cost*child_nodes[x]->densities[*iter]);
      } else {
        costs[*iter] = cost*child_nodes[x]->densities[*iter];
      }
    }
  }
  node->cardinality = costs[join_key];

  star_solution->root = node;

  return star_solution;
}

QueryPlanner::QuerySolution* QueryPlanner::buildJoin(const QueryGraph& graph, const QueryPattern* pattern, const std::vector<QuerySolution*>& child_solutions) {
  std::vector<std::vector<QuerySolution*>> dp_table(child_solutions.size()+1);
  std::map<BitSet, QuerySolution*> lookup;
  for(int i=0; i<child_solutions.size(); i++) {
    dp_table[1].push_back(child_solutions[i]);
  }

  for(int i=2; i<=child_solutions.size(); i++) {
    for(int j=0; j<dp_table[i-1].size(); j++) {
      for(int k=0; k<dp_table[1].size(); k++) {
        BitSet node_intersect = dp_table[i-1][j]->nodes & dp_table[1][k]->nodes;
        if(!node_intersect.any()) {
          PlanNode* left = dp_table[i-1][j]->root;
          while(left != nullptr) {
            PlanNode* right = dp_table[1][k]->root;

            BitSet left_res = left->available_res;
            BitSet right_res = right->available_res;

            BitSet res_intersect = left_res & right_res;
            if(!res_intersect.any()) {
              left = left->next;
              continue;
            }
            uint32_t join_key = *res_intersect.begin();

            BitSet node_union = dp_table[i-1][j]->nodes | dp_table[1][k]->nodes;
            QuerySolution *solution;
            bool exist =false;
            if(lookup.count(node_union) != 0) {
              solution = lookup[node_union];
              exist = true;
            } else {
              solution = solution_pool.alloc();
            }

            PlanNode* node;
            std::vector<PlanNode*> child_nodes;
            std::vector<JoinNode> join_nodes;
            std::vector<uint32_t> join_res;

            if(left->op == PlanNode::BackProbeHashJoin) {
              child_nodes.push_back(right);
              join_nodes.push_back(JoinNode(UINT32_MAX, join_key));
              int m = child_nodes.size();

              join_res.push_back(join_key);

              child_nodes.insert(child_nodes.end(), left->child_nodes.begin(), left->child_nodes.end());
              join_nodes.insert(join_nodes.end(), left->join_nodes.begin(), left->join_nodes.end());
              BitSet available_res = left_res|right_res;
              join_res.insert(join_res.end(), left->join_res.begin(), left->join_res.end());
              for(int x=0; x<left->child_nodes.size(); ++x) {
                if(left->child_nodes[x]->available_res.test(join_key)) {
                  join_nodes[x+m].left_join_key = join_key;
                }
              }
              node = createBackProbeHashJoin(child_nodes, join_nodes, join_res, available_res);
              node->costs = child_nodes[0]->costs + child_nodes[0]->cardinality;
              std::unordered_map<uint32_t, double> costs;
              for(BitSet::SetBitIterator iter = child_nodes[0]->available_res.begin(); iter != child_nodes[0]->available_res.end(); ++iter) {
                costs[*iter] = child_nodes[0]->cardinality*child_nodes[0]->densities[*iter];
              }
              double cost = 0;
              for(int x=1; x<child_nodes.size(); ++x) {
                cost = child_nodes[x]->cardinality;
                for(BitSet::SetBitIterator iter = child_nodes[x]->available_res.begin(); iter != child_nodes[x]->available_res.end(); ++iter) {
                  if(costs.count(*iter)) {
                    cost = std::min(cost, costs[*iter]/child_nodes[x]->densities[*iter]);
                  }
                }
                node->costs += child_nodes[x]->costs + cost;
                for(BitSet::SetBitIterator iter = child_nodes[x]->available_res.begin(); iter != child_nodes[x]->available_res.end(); ++iter) {
                  if(costs.count(*iter)) {
                    costs[*iter] = std::min(costs[*iter], cost*child_nodes[x]->densities[*iter]);
                  } else {
                    costs[*iter] = cost*child_nodes[x]->densities[*iter];
                  }
                }
              }
              node->cardinality = cost;
            } else {
              /*
              if(right->op == PlanNode::BackProbeHashJoin) {
                child_nodes.insert(child_nodes.end(), right->child_nodes.begin(), right->child_nodes.end());
                join_nodes.insert(join_nodes.end(), right->join_nodes.begin(), right->join_nodes.end());
                for(int x=0; x<join_nodes.size(); ++x) {
                  if(child_nodes[x]->available_res.test(join_key)) {
                    join_nodes[x].right_join_key = join_key;
                  }
                }
                join_res.insert(join_res.end(), right->join_res.begin(), right->join_res.end());
              } else {
                child_nodes.push_back(right);
                join_nodes.push_back(JoinNode(UINT32_MAX, join_key));
              }
              */
              child_nodes.push_back(right);
              join_nodes.push_back(JoinNode(UINT32_MAX, join_key));

              join_res.push_back(join_key);

              child_nodes.push_back(left);
              join_nodes.push_back(JoinNode(UINT32_MAX, join_key));
              BitSet available_res = left_res|right_res;

              node = createBackProbeHashJoin(child_nodes, join_nodes, join_res, available_res);
              double left_cost = std::min(left->cardinality, right->cardinality*right->densities[join_key]/left->densities[join_key]);
              node->costs = right->costs + right->cardinality + left->costs + left_cost;
              node->cardinality = left_cost;
            }


            if(exist){
              node->next = solution->root;
              solution->root = node;
            } else {
              solution->root = node;
              solution->nodes = node_union;
              dp_table[i].push_back(solution);
              lookup[node_union] = solution;
            }

            left = left->next;
          }

        }
      }
    }
  }

  return dp_table[child_solutions.size()][0];
}


QueryPlanner::QuerySolution* QueryPlanner::buildScan(const QueryGraph& graph, int node_index) {
  QuerySolution *solution = solution_pool.alloc();
  solution->nodes = BitSet(graph.numOfNodes());
  solution->nodes.set(node_index);

  const QueryNode& query_node = graph.getNode(node_index);

  solution->root = createTableScanNode(node_index, query_node, graph.numOfVariables());

  return solution;
}

PlanNode* QueryPlanner::createHashJoin(const std::vector<PlanNode*>& child_nodes, const std::vector<JoinNode>& join_nodes, const std::vector<uint32_t>& join_res, BitSet& available_res) {
  PlanNode* node = plan->createPlanNode();
  node->op = PlanNode::HashJoin;
  node->next = nullptr;
  node->child_nodes = child_nodes;
  node->join_nodes = join_nodes;
  node->join_res = join_res;
  node->available_res = available_res;
  node->densities = std::map<uint32_t, double>();
  for(int i=0; i<child_nodes.size(); ++i) {
    for (std::map<uint32_t, double>::iterator it = child_nodes[i]->densities.begin(); it != child_nodes[i]->densities.end(); ++it) {
      if(node->densities.count(it->first)) {
        node->densities[it->first] = std::max(node->densities[it->first], it->second);
      } else {
        node->densities[it->first] = it->second;
      }
    }
  }
  return node;
}

PlanNode* QueryPlanner::createBackProbeHashJoin(const std::vector<PlanNode*>& child_nodes, const std::vector<JoinNode>& join_nodes, const std::vector<uint32_t>& join_res, BitSet& available_res) {
  PlanNode* node = plan->createPlanNode();
  node->op = PlanNode::BackProbeHashJoin;
  node->next = nullptr;
  node->child_nodes = child_nodes;
  node->join_nodes = join_nodes;
  node->join_res = join_res;
  node->available_res = available_res;
  node->densities = std::map<uint32_t, double>();
  for(int i=0; i<child_nodes.size(); ++i) {
    for (std::map<uint32_t, double>::iterator it = child_nodes[i]->densities.begin(); it != child_nodes[i]->densities.end(); ++it) {
      if(node->densities.count(it->first)) {
        node->densities[it->first] = std::max(node->densities[it->first], it->second);
      } else {
        node->densities[it->first] = it->second;
      }
    }
  }
  return node;
}

PlanNode* QueryPlanner::createTableScanNode(int node_index, const QueryNode& query_node, int total_num_variables) {
  PlanNode* node = plan->createPlanNode();
  node->op = PlanNode::TableScan;
  node->next = nullptr;
  node->node_index = node_index;
  node->query_node = query_node;
  node->ordering = -1;

  if(query_node.subject.type != QueryResource::Variable) {
    node->triple_order = TripleOrder::SP;
    node->cardinality = stat_manager.getCount(TripleOrder::SP, query_node.subject.id, query_node.predicate.id, 0);
    node->costs = LowerBoundsCostModel::estimateTableScan((node->cardinality*sizeof(uint32_t)+BLOCK_SIZE)/BLOCK_SIZE);
    node->densities = std::map<uint32_t, double>();
    node->densities[query_node.object.id] = 1.0*stat_manager.getDistinctCount(TripleOrder::OP, 0, query_node.predicate.id, 0)/stat_manager.getCount(TripleOrder::P, 0, query_node.predicate.id, 0);
    node->available_res = BitSet(total_num_variables);
    node->available_res.set(query_node.object.id);
  } else if(query_node.object.type != QueryResource::Variable) {
    node->triple_order = TripleOrder::OP;
    node->cardinality = stat_manager.getCount(TripleOrder::OP, 0, query_node.predicate.id, query_node.object.id);
    node->costs = LowerBoundsCostModel::estimateTableScan((node->cardinality*sizeof(uint32_t)+BLOCK_SIZE)/BLOCK_SIZE);
    node->densities = std::map<uint32_t, double>();
    node->densities[query_node.subject.id] = 1.0*stat_manager.getDistinctCount(TripleOrder::SP, 0, query_node.predicate.id, 0)/stat_manager.getCount(TripleOrder::P, 0, query_node.predicate.id, 0);
    node->available_res = BitSet(total_num_variables);
    node->available_res.set(query_node.subject.id);
  } else {
    node->triple_order = TripleOrder::P;
    node->cardinality = stat_manager.getCount(TripleOrder::P, 0, query_node.predicate.id, 0);
    node->costs = LowerBoundsCostModel::estimateTableScan((node->cardinality*sizeof(uint32_t)*2+BLOCK_SIZE)/BLOCK_SIZE);
    node->densities = std::map<uint32_t, double>();
    node->densities[query_node.subject.id] = 1.0*stat_manager.getDistinctCount(TripleOrder::SP, 0, query_node.predicate.id, 0)/node->cardinality;
    node->densities[query_node.object.id] = 1.0*stat_manager.getDistinctCount(TripleOrder::OP, 0, query_node.predicate.id, 0)/node->cardinality;
    node->available_res = BitSet(total_num_variables);
    node->available_res.set(query_node.subject.id);
    node->available_res.set(query_node.object.id);
  }

  if(query_node.subject.id == query_node.object.id) {
    node = createFilterNode(query_node.subject.id, 0, node);
  }
  return node;
}

PlanNode* QueryPlanner::createFilterNode(uint32_t filter_key, uint32_t filter_value, PlanNode* child) {
  PlanNode* node = plan->createPlanNode();
  node->op = PlanNode::Filter;
  node->next = nullptr;
  node->child_nodes.push_back(child);
  node->filter_key = filter_key;
  node->filter_value = filter_value;
  node->available_res = child->available_res;
  node->costs = child->costs + child->cardinality;
  node->cardinality = child->cardinality;
  return node;
}

PlanNode* QueryPlanner::createResultsPrinterNode(const QueryGraph& graph, PlanNode* child) {
  PlanNode* node = plan->createPlanNode();
  node->op = PlanNode::ResultsPrinter;
  node->next = nullptr;
  node->child_nodes.push_back(child);
  node->projection = graph.getProjection();
  node->costs = child->costs + child->cardinality;
  node->cardinality = child->cardinality;
  return node;
}

void QueryPlanner::bindResource(PlanNode* node, const std::set<uint32_t>& required_res) {
  node->required_res = required_res;
  std::set<uint32_t> res = required_res;
  if(node->op == PlanNode::ResultsPrinter) {
    for(int i=0; i<node->projection.size(); i++) {
      res.insert(node->projection[i].id);
    }
  } else if(node->op == PlanNode::HashJoin || node->op == PlanNode::BackProbeHashJoin) {
    for(int i=1; i<node->join_nodes.size(); i++) {
      if(node->join_nodes[i].left_join_key != UINT32_MAX) {
        res.insert(node->join_nodes[i].left_join_key);
      }
      if(node->join_nodes[i].right_join_key != UINT32_MAX) {
        res.insert(node->join_nodes[i].right_join_key);
      }
    }
  }

  for(int i=0; i<node->child_nodes.size(); i++) {
    bindResource(node->child_nodes[i], res);
  }
}

void QueryPlanner::optimize(PlanNode* node) {
  optimizeJoin(node);
}

void QueryPlanner::optimizeJoin(PlanNode* node) {
  if(node->op == PlanNode::HashJoin || node->op == PlanNode::BackProbeHashJoin) {
    double cardinality = node->child_nodes[0]->cardinality;
    for(int i=1; i<node->child_nodes.size(); i++) {
      PlanNode* child_node = node->child_nodes[i];
      if(child_node->op == PlanNode::TableScan && child_node->triple_order == TripleOrder::P) {
        node->join_nodes[i].can_filter = true;
        if(child_node->query_node.subject.id == node->join_nodes[i].left_join_key) {
          node->join_nodes[i].left_res_pos = ResourcePosition::SUBJECT;
        } else {
          node->join_nodes[i].left_res_pos = ResourcePosition::OBJECT;
        }

        if(child_node->query_node.subject.id == node->join_nodes[i].right_join_key) {
          node->join_nodes[i].right_res_pos = ResourcePosition::SUBJECT;
        } else {
          node->join_nodes[i].right_res_pos = ResourcePosition::OBJECT;
        }
      }
    }
  }
  for(int i=0; i<node->child_nodes.size(); i++) {
    optimizeJoin(node->child_nodes[i]);
  }
}
