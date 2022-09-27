#ifndef RESOURCE_H
#define RESOURCE_H

#include <cstdint>
#include <string>
#include <vector>

enum ResourcePosition {
  SUBJECT = 1, PREDICATE = 2, OBJECT = 4
};

struct Resource {
  uint32_t id;
  std::string literal;
  std::vector<uint32_t> column;


  Resource() {}
  Resource(uint32_t id) : id(id) {}
  Resource(uint32_t id, std::string& literal) : id(id), literal(literal) {}
};

#endif
