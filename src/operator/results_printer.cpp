#include "results_printer.h"


ResultsPrinter::ResultsPrinter(Dictionary& dict, const std::vector<std::string>& projection, Operator* input, const std::vector<Resource*>& resources, std::ostream& out, double expected_cardinality, bool silent)
  : Operator(expected_cardinality), dict(dict), projection(projection), input(input), resources(resources), out(out), silent(silent), result_count(0) {}

ResultsPrinter::~ResultsPrinter() {}

void ResultsPrinter::open() {
  this->input->open();
}

void ResultsPrinter::close() {
  this->input->close();
}

bool ResultsPrinter::first() {
  if(!silent) {
    for(std::vector<std::string>::iterator it = projection.begin(), end = projection.end(); it != end; ++it) {
      out << *it << "\t";
    }
    out << "\n";
    out << "--------------------------------------------------------------------------------\n";
  }
  if(this->input->first()) {
    if(resources.size() != 0) {
      int count = resources[0]->column.size();
      result_count += count;
      if(!silent) {
        for(int i=0; i<count; i++) {
          for(int j=0; j<resources.size(); j++) {
            std::string literal;
            dict.lookupById(resources[j]->column[i], &resources[j]->literal);
            out << resources[j]->literal << "  ";
            // out << resources[j]->column[i] << "  ";
          }
          out << "\n";
        }
      }
      for(int i=0; i<resources.size(); ++i) {
        resources[i]->column.clear();
      }
    }
    return true;
  } else {
    return false;
  }
}

bool ResultsPrinter::next() {
  while(this->input->next()) {
    if(resources.size() != 0) {
      int count = resources[0]->column.size();
      result_count += count;
      if(!silent) {
        for(int i=0; i<count; i++) {
          for(int j=0; j<resources.size(); j++) {
            std::string literal;
            dict.lookupById(resources[j]->column[i], &resources[j]->literal);
            out << resources[j]->literal << "  ";
            // out << resources[j]->column[i] << "  ";
          }
          out << "\n";
        }
      }
      for(int i=0; i<resources.size(); i++) {
        resources[i]->column.clear();
      }
    }
    return true;
  }
  out << "Total results: " << result_count << "\n";
  return false;
}
