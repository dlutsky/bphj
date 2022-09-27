#ifndef RESULTS_PRINTER_H
#define RESULTS_PRINTER_H

#include <string>
#include <iostream>
#include <vector>
#include "operator.h"
#include "storage/dictionary.h"


class ResultsPrinter : public Operator {
public:
  ResultsPrinter(Dictionary& dict, const std::vector<std::string>& projection, Operator* input, const std::vector<Resource*>& resources, std::ostream& out, double expected_cardinality, bool silent=false);
  ~ResultsPrinter();

  void open();
  void close();

  bool first();
  bool next();

protected:
  Dictionary& dict;
  std::vector<std::string> projection;

  Operator* input;
  std::vector<Resource*> resources;

  std::ostream& out;

  bool silent;

  int result_count;
};


#endif
