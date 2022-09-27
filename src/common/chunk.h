#ifndef CHUNK_H
#define CHUNK_H

#include <cstring>
#include <sys/types.h>


template <class T, class SizeOf>
class Chunk {
public:
  Chunk(SizeOf size_of);
  Chunk(const char* data, size_t size, SizeOf size_of);
  ~Chunk();

  class Iterator {
  public:
    Iterator(const char* data);
    Iterator(const char* data, size_t pos);
    Iterator(const Iterator& iter);
    const T& operator*();
    const T* operator&();
    const T* operator->();
    Iterator& operator++();
    bool operator==(const Iterator& other) const;
    bool operator!=(const Iterator& other) const;
    bool operator<(const Iterator& other) const;

  private:
    const char* data;
    size_t pos;
  };

  void append(const T& val);
  void clear();

  Iterator begin() const;
  Iterator end() const;

private:
  char* data_ptr;
  size_t data_size;
  size_t capacity;
  SizeOf size_of;
};


template <class T, class SizeOf>
Chunk<T, SizeOf>::Chunk(const char* data, size_t size, SizeOf size_of) : data_size(size), capacity(size), size_of(size_of) {
  data_ptr = new char[size];
  memcpy(data_ptr, data, size);
}

template <class T, class SizeOf>
Chunk<T, SizeOf>::~Chunk() {
  delete[] data_ptr;
}



template <class T, class SizeOf>
Chunk<T, SizeOf>::Iterator::Iterator(const char* data) : data(data), pos(0) {}

template <class T, class SizeOf>
Chunk<T, SizeOf>::Iterator::Iterator(const char* data, size_t pos) : data(data), pos(pos) {}

template <class T, class SizeOf>
Chunk<T, SizeOf>::Iterator::Iterator(const Chunk<T, SizeOf>::Iterator& iter) : data(iter.data), pos(iter.pos) {}

template <class T, class SizeOf>
const T& Chunk<T, SizeOf>::Iterator::operator*() {
  return *reinterpret_cast<const T*>(data+pos);
}

template <class T, class SizeOf>
const T* Chunk<T, SizeOf>::Iterator::operator&() {
  return reinterpret_cast<const T*>(data+pos);
}

template <class T, class SizeOf>
const T* Chunk<T, SizeOf>::Iterator::operator->() {
  return reinterpret_cast<const T*>(data+pos);
}

template <class T, class SizeOf>
typename Chunk<T, SizeOf>::Iterator& Chunk<T, SizeOf>::Iterator::operator++() {
  pos += size_of(reinterpret_cast<const T*>(data+pos));
  return *this;
}

template <class T, class SizeOf>
bool Chunk<T, SizeOf>::Iterator::operator==(const typename Chunk<T, SizeOf>::Iterator& other) const {
  return pos==other.pos && data==other.data;
}

template <class T, class SizeOf>
bool Chunk<T, SizeOf>::Iterator::operator!=(const typename Chunk<T, SizeOf>::Iterator& other) const {
  return pos!=other.pos || data!=other.data;
}

template <class T, class SizeOf>
bool Chunk<T, SizeOf>::Iterator::operator<(const typename Chunk<T, SizeOf>::Iterator& other) const {
  return pos<other.pos;
}



template <class T, class SizeOf>
typename Chunk<T, SizeOf>::Iterator Chunk<T, SizeOf>::begin() const {
  return Iterator(data_ptr);
}

template <class T, class SizeOf>
typename Chunk<T, SizeOf>::Iterator Chunk<T, SizeOf>::end() const {
  return Iterator(data_ptr, data_size);
}

#endif
