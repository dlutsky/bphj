#ifndef OPERATOR_H
#define OPERATOR_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include "common/resource.h"
#include "common/triple.h"


class Operator {
public:
  virtual ~Operator();

  virtual void open() = 0;
  virtual void close() = 0;

  //virtual bool first(std::vector<int>& counts) = 0;
  //virtual bool next(std::vector<int>& counts) = 0;

  virtual bool first() = 0;
  virtual bool next() = 0;

  double getExpectedCardinality();
  void setTripleOrder(TripleOrder triple_order);

protected:
  Operator(double expected_cardinality);
  Operator(double expected_cardinality, TripleOrder triple_order);

  double expected_cardinality;
  TripleOrder triple_order;
};



#endif
