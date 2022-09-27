#ifndef RUNTIME_H
#define RUNTIME_H

#include <string>
#include <vector>
#include <list>
#include <iostream>
#include <fstream>
#include "storage/dictionary.h"
#include "operator/operator.h"
#include "database/database.h"


class CodeGenerator;

class Runtime {
public:
  Runtime(Database& db);
  Runtime(Database& db, const std::string& out_file_path);
  ~Runtime();

  Operator* moveToPool(Operator*& op);
  Resource* createResource();
  std::ostream& getOstream();

private:
  friend class CodeGenerator;

  Database& db;

  std::ofstream of;
  std::ostream& out;

  std::list<Resource> resources;
  std::list<Operator*> operators;
  Operator* operator_tree;
};




#endif
