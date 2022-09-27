#ifndef BASIC_TYPES_H
#define BASIC_TYPES_H


struct IntegerSizeOf {
  size_t operator() (uint32_t* a) {
    return sizeof(uint32_t);
  }
  size_t operator() (uint32_t a) {
    return sizeof(uint32_t);
  }
};



#endif
