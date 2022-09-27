#ifndef REGISTRY_H
#define REGISTRY_H

#include <cstdint>


class Registry {
public:
  Registry();
  ~Registry();
  
private:
  char* data;
  int size;
};



#endif
