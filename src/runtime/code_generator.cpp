#include "code_generator.h"
#include "operator/filter.h"
#include "operator/hash_join.h"
#include "operator/backprobe_hash_join.h"
#include "operator/table_scan.h"
#include "operator/results_printer.h"

#include <iostream>


CodeGenerator::CodeGenerator(Runtime& runtime, bool silent) : runtime(runtime), silent(silent) {}

CodeGenerator::~CodeGenerator() {}

Operator* CodeGenerator::generate(const PlanNode* plan_node) {
  std::map<uint32_t, Resource*> resources;
  return generateInternal(plan_node, resources);
}

Operator* CodeGenerator::generate(Runtime& runtime, const PlanNode* plan_node, bool silent) {
  CodeGenerator generator(runtime, silent);
  return generator.generate(plan_node);
}

Operator* CodeGenerator::generateInternal(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources) {
  switch(plan_node->op) {
    case PlanNode::ResultsPrinter:
      return generateResultsPrinter(plan_node, resources);
    case PlanNode::BackProbeHashJoin:
      return generateBackProbeHashJoin(plan_node, resources);
    case PlanNode::HashJoin:
      return generateBackProbeHashJoin(plan_node, resources);
    case PlanNode::TableScan:
      return generateTableScan(plan_node, resources);
    case PlanNode::Filter:
      return generateFilter(plan_node, resources);
  }
  return nullptr;
}

Operator* CodeGenerator::generateResultsPrinter(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources) {
  Operator* child = generateInternal(plan_node->child_nodes[0], resources);

  std::vector<std::string> projection(plan_node->projection.size());
  std::vector<Resource*> rcs(plan_node->projection.size());
  for(int i=0; i<plan_node->projection.size(); i++) {
    projection[i] = plan_node->projection[i].value;
    rcs[i] = resources[plan_node->projection[i].id];
  }
  Operator* opt = new ResultsPrinter(runtime.db.getDictionary(), projection, child, rcs, runtime.getOstream(), plan_node->cardinality, this->silent);
  return opt;
}

Operator* CodeGenerator::generateBackProbeHashJoin(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources) {
  std::vector<Operator*> opts;
  // input resources
  std::vector<std::map<uint32_t, Resource*>> rcs;
  for(int i=0; i<plan_node->child_nodes.size(); i++) {
    std::map<uint32_t, Resource*> r;
    opts.push_back(generateInternal(plan_node->child_nodes[i], r));
    rcs.push_back(r);
  }

  // output resources
  std::map<uint32_t, Resource*> res;
  for(int i=rcs.size()-1; i>=0; i--) {
    for(std::map<uint32_t, Resource*>::iterator iter = rcs[i].begin(); iter != rcs[i].end(); ++iter) {
      if(res.count(iter->first) != 0) {
        continue;
      }
      if(plan_node->required_res.count(iter->first) != 0) {
        Resource* r = runtime.createResource();
        res[iter->first] = r;
      }
    }
  }

  resources.insert(res.begin(), res.end());

  Operator* opt = new BackProbeHashJoin(opts, rcs, res, plan_node->join_nodes, plan_node->cardinality);
  return opt;
}

Operator* CodeGenerator::generateTableScan(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources) {
  Resource* subject = nullptr;
  Resource* predicate = nullptr;
  Resource* object = nullptr;

  switch(plan_node->triple_order) {
    case TripleOrder::SP:
      subject = runtime.createResource();
      subject->id = plan_node->query_node.subject.id;
      subject->literal = plan_node->query_node.subject.value;
      if(plan_node->required_res.count(plan_node->query_node.subject.id) != 0) {
        resources[subject->id] = subject;
      }

      predicate = runtime.createResource();
      predicate->id = plan_node->query_node.predicate.id;
      predicate->literal = plan_node->query_node.predicate.value;
      if(plan_node->required_res.count(plan_node->query_node.predicate.id) != 0) {
        resources[predicate->id] = predicate;
      }

      if(plan_node->required_res.count(plan_node->query_node.object.id) != 0) {
        object = runtime.createResource();
        resources[plan_node->query_node.object.id] = object;
      }
      break;
    case TripleOrder::OP:
      if(plan_node->required_res.count(plan_node->query_node.subject.id) != 0) {
        subject = runtime.createResource();
        resources[plan_node->query_node.subject.id] = subject;
      }

      predicate = runtime.createResource();
      predicate->id = plan_node->query_node.predicate.id;
      predicate->literal = plan_node->query_node.predicate.value;
      if(plan_node->required_res.count(plan_node->query_node.predicate.id) != 0) {
        resources[predicate->id] = predicate;
      }

      object = runtime.createResource();
      object->id = plan_node->query_node.object.id;
      object->literal = plan_node->query_node.object.value;
      if(plan_node->required_res.count(plan_node->query_node.object.id) != 0) {
        resources[object->id] = object;
      }
      break;
    case TripleOrder::P:
      if(plan_node->required_res.count(plan_node->query_node.subject.id) != 0) {
        subject = runtime.createResource();
        resources[plan_node->query_node.subject.id] = subject;
      }

      predicate = runtime.createResource();
      predicate->id = plan_node->query_node.predicate.id;
      predicate->literal = plan_node->query_node.predicate.value;
      if(plan_node->required_res.count(plan_node->query_node.predicate.id) != 0) {
        resources[predicate->id] = predicate;
      }

      if(plan_node->required_res.count(plan_node->query_node.object.id) != 0) {
        object = runtime.createResource();
        resources[plan_node->query_node.object.id] = object;
      }
      break;
  }

  Operator* opt = new TableScan(runtime.db.getTripleTable(), plan_node->triple_order, subject, predicate, object, plan_node->cardinality);
  return opt;
}

Operator* CodeGenerator::generateFilter(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources) {
  Operator* child = generateInternal(plan_node->child_nodes[0], resources);
  Operator* opt = nullptr;

  Resource* res = runtime.createResource();
  res->id = plan_node->filter_value;
  opt = new Filter(child, resources[plan_node->filter_key], res, plan_node->cardinality);

  return opt;
}
