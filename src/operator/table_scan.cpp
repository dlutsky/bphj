#include "table_scan.h"
#include <iostream>

TableScan::TableScan(TripleTable& table, TripleOrder triple_order, Resource* subject, Resource* predicate, Resource* object, double expected_cardinality)
  : Operator(expected_cardinality, triple_order), table(table), subject(subject), predicate(predicate), object(object), scanner(nullptr) {}

TableScan::~TableScan() {
  if(scanner != nullptr) {
    delete scanner;
  }
}

void TableScan::open() {}

void TableScan::close() {}

bool TableScan::first() {
  scanner = new TripleTable::BlockScanner(this->table, triple_order, subject, predicate, object);
  if(!scanner->find()) {
    return false;
  }
  return scanner->next();
}

bool TableScan::next() {
  return scanner->next();
}
