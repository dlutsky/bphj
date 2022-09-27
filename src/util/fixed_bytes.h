#ifndef FIXED_BYTES_H
#define FIXED_BYTES_H

#include <cstddef>

template <size_t N>
class FixedBytes {
public:
  FixedBytes();
  ~FixedBytes();

  char* data();
  const char* data() const;
  size_t size();

private:
  char* bytes_data;
  size_t bytes_size;
};


#endif
