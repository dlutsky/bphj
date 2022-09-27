#ifndef BYTE_BUFFER_H
#define BYTE_BUFFER_H

#include <sys/types.h>

class ByteBuffer {
public:
  ByteBuffer();
  ByteBuffer(const char* data, size_t size);
  ~ByteBuffer();

  ByteBuffer(const ByteBuffer& other);

  void reserve(size_t capacity);
  void resize(size_t size);

  size_t size() const;
  const char* data() const;
  char* data();
  void append(const char* data, size_t size);

private:
  void destroy();

  size_t capacity;
  size_t buf_size;
  char* buf_data;
};


#endif
