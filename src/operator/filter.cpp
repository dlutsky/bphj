#include "filter.h"

Filter::Filter(Operator* input, Resource* value, Resource* filter, double expected_cardinality)
  : Operator(expected_cardinality), input(input), value(value), filter(filter) {}

Filter::~Filter() {}

void Filter::open() {
  this->input->open();
}

void Filter::close() {
  this->input->close();
}

bool Filter::first() {
  if(!filter->column.empty()) {
    for(int i=0; i<filter->column.size(); i++) {
      this->valid.insert(filter->column[i]);
    }
  } else {
    this->valid.insert(filter->id);
  }
  if(!this->input->first()) {
    return false;
  }
  return true;
}

bool Filter::next() {
  return true;
}
