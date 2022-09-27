#include "fixed_bytes.h"


template <size_t N>
FixedBytes::FixedBytes() {
  bytes_size = N;
  bytes_data = new char[bytes_size];
}

~FixedBytes() {
  delete[] bytes_data;
}

char* FixedBytes::data() {
  return bytes_data;
}

const char* FixedBytes::data() const {
  return bytes_data;
}

size_t FixedBytes::size() {
  return bytes_size;
}
