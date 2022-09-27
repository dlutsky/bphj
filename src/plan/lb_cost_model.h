#ifndef LOWER_BOUNDS_COST_MODEL_H
#define LOWER_BOUNDS_COST_MODEL_H

#include "common/constants.h"

#ifndef TYPEDEF_COST_T
#define TYPEDEF_COST_T
typedef double cost_t;
#endif


class LowerBoundsCostModel {
public:
  static cost_t estimateTableScan(int pages) {
    return pages*page_scan_costs;
  }

  static cost_t estimateFilter(double card) {
    return card;
  }

private:
  static const cost_t page_scan_costs;
  static const cost_t kv_lookup_costs;
};

const cost_t LowerBoundsCostModel::page_scan_costs = 17;



#endif
