#include <cstring>
#include "byte_buffer.h"


ByteBuffer::ByteBuffer() : capacity(0), buf_size(0), buf_data(nullptr) {}

ByteBuffer::ByteBuffer(const char* data, size_t size) : capacity(size), buf_size(size) {
  this->buf_data = new char[size];
  memcpy(this->buf_data, data, size);
}

ByteBuffer::~ByteBuffer() {
  destroy();
}

ByteBuffer::ByteBuffer(const ByteBuffer& other) {
  reserve(other.capacity);
  this->buf_size = other.buf_size;
  memcpy(this->buf_data, other.buf_data, other.buf_size);
}

void ByteBuffer::reserve(size_t capacity) {
  this->capacity = capacity;
  if(this->buf_data != nullptr) {
    char* temp = this->buf_data;
    this->buf_data = new char[capacity];
    memcpy(this->buf_data, temp, this->buf_size);
    delete[] temp;
  } else {
    this->buf_data = new char[capacity];
  }
}

void ByteBuffer::resize(size_t size) {
  if(size<=this->capacity) {
    this->buf_size = size;
  }
}

size_t ByteBuffer::size() const {
  return this->buf_size;
}

const char* ByteBuffer::data() const {
  return this->buf_data;
}

char* ByteBuffer::data() {
  return this->buf_data;
}

void ByteBuffer::append(const char* data, size_t size) {
  if(this->buf_size+size > this->capacity) {
    reserve((this->buf_size+size)*2);
  }
  memcpy(this->buf_data + this->buf_size, data, size);
  this->buf_size += size;
}

void ByteBuffer::destroy() {
  this->capacity = 0;
  this->buf_size = 0;
  if(this->buf_data) {
    delete[] this->buf_data;
  }
}
