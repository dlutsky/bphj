#include <iostream>
#include "query_plan.h"
#include "operator/operator.h"
#include "runtime/code_generator.h"
#include "runtime/runtime.h"



QueryPlan::QueryPlan() {}

QueryPlan::~QueryPlan() {}

void QueryPlan::execute(Runtime& runtime, bool silent) {
  Operator* operator_tree = CodeGenerator::generate(runtime, this->root, silent);
  operator_tree->open();
  if(operator_tree->first()) {
    while(operator_tree->next());
  }
  operator_tree->close();
}

void QueryPlan::print() {
  print(this->root, 0);
}

std::ostream& operator<<(std::ostream& out, PlanNode* node) {
  switch(node->op) {
    case PlanNode::TableScan:
      out << "->  Table scan";
      out << "[" << "pattern=" << node->node_index << " order=" << node->triple_order << " cardinality=" << node->cardinality << " cost=" << node->costs << "]";
      break;
    case PlanNode::Filter:
      out << "->  Filter";
      out << "[" << "cardinality=" << node->cardinality << " cost=" << node->costs << "]";
      break;
    case PlanNode::HashJoin:
      out << "->  Hash join";
      out << "[" << "cardinality=" << node->cardinality << " cost=" << node->costs << "]";
      break;
    case PlanNode::BackProbeHashJoin:
      out << "->  Back-probe Hash join";
      out << "[" << "cardinality=" << node->cardinality << " cost=" << node->costs << "]";
      break;
    case PlanNode::ResultsPrinter:
      out << "->  Results printer";
      out << "[" << "cardinality=" << node->cardinality << " cost=" << node->costs << "]";
      break;
  }
}

void QueryPlan::print(PlanNode* node, int deep) {
  for(int i=0; i<deep*4; i++) {
    std::cout << " ";
  }
  std::cout << node << std::endl;
  for(std::vector<PlanNode*>::iterator it = node->child_nodes.begin(), end = node->child_nodes.end(); it != end; ++it) {
    print(*it, deep+1);
  }
}

PlanNode* QueryPlan::createPlanNode() {
  PlanNode* node = this->node_pool.alloc();
  node->next = nullptr;
  return node;
}

void QueryPlan::freePlanNode(PlanNode* node) {
  this->node_pool.free(node);
}
