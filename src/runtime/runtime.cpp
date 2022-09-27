#include "runtime.h"
#include "database/database.h"

Runtime::Runtime(Database& db) : db(db), out(std::cout) {}

Runtime::Runtime(Database& db, const std::string& out_file_path)
  : db(db), of(out_file_path), out(of) {}

Runtime::~Runtime() {
  for(std::list<Operator*>::iterator it = operators.begin(), end = operators.end(); it != end; ++it) {
    delete *it;
  }
}

Operator* Runtime::moveToPool(Operator*& op) {
  operators.push_back(op);
  op = nullptr;
  return operators.back();
}

Resource* Runtime::createResource() {
  resources.push_back(Resource());
  return &resources.back();
}

std::ostream& Runtime::getOstream() {
  return out;
}
