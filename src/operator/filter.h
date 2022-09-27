#ifndef FILTER_H
#define FILTER_H

#include <unordered_set>
#include "operator.h"

class Filter : public Operator {
public:
  Filter(Operator* input, Resource* value, Resource* filter, double expected_cardinality);
  ~Filter();

  void open();
  void close();

  bool first();
  bool next();

protected:
  Operator* input;
  Resource *value, *filter;

  std::unordered_set<uint32_t> valid;
};


#endif
