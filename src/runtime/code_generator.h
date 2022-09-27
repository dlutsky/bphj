#ifndef CODE_GENERATOR_H
#define CODE_GENERATOR_H

#include <vector>
#include <map>
#include <unordered_map>
#include "common/resource.h"
#include "plan/query_plan.h"
#include "operator/operator.h"
#include "runtime.h"


class CodeGenerator {
public:
  CodeGenerator(Runtime& runtime, bool silent);
  ~CodeGenerator();

  Operator* generate(const PlanNode* plan_node);

  static Operator* generate(Runtime& runtime, const PlanNode* plan_node, bool silent);

private:
  Operator* generateInternal(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources);
  Operator* generateBackProbeHashJoin(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources);
  Operator* generateResultsPrinter(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources);
  Operator* generateTableScan(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources);
  Operator* generateStoreScan(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources);
  Operator* generateFilter(const PlanNode* plan_node, std::map<uint32_t, Resource*>& resources);

  Runtime& runtime;
  bool silent;
};

#endif
