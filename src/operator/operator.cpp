#include "operator.h"


Operator::Operator(double expected_cardinality) : expected_cardinality(expected_cardinality) {}

Operator::Operator(double expected_cardinality, TripleOrder triple_order) : expected_cardinality(expected_cardinality), triple_order(triple_order) {}

Operator::~Operator() {}

double Operator::getExpectedCardinality() {
  return expected_cardinality;
}

void Operator::setTripleOrder(TripleOrder triple_order) {
  this->triple_order = triple_order;
}
