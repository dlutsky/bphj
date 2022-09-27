#ifndef TABLE_SCAN_H
#define TABLE_SCAN_H

#include <memory>
#include <vector>
#include "operator.h"
#include "storage/triple_table.h"

class TableScan : public Operator {
public:
  TableScan(TripleTable& table, TripleOrder triple_order, Resource* subject, Resource* predicate, Resource* object, double expected_cardinality);
  ~TableScan();

  void open();
  void close();

  void consume(int idx);
  void produce(Operator* parent, int idx);

  bool first();
  bool next();

protected:
  TripleTable& table;
  Resource *subject, *predicate, *object;

  TripleTable::BlockScanner* scanner;
};

#endif
