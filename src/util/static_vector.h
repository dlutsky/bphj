#ifndef STATIC_VECTOR_H
#define STATIC_VECTOR_H

#include <sys/types.h>


template <typename T>
class StaticVector {
public:
  StaticVector(const char* pool, size_t size);

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

  Iterator begin() const;
  Iterator end() const;
  Iterator find(size_t pos) const;

private:
  const char* pool;
  size_t size;
};




template <typename T>
StaticVector<T>::StaticVector(const char* pool, size_t size) : pool(pool), size(size) {}



template <typename T>
StaticVector<T>::Iterator::Iterator(const char* data) : data(data), pos(0) {}

template <typename T>
StaticVector<T>::Iterator::Iterator(const char* data, size_t pos) : data(data), pos(pos) {}

template <typename T>
StaticVector<T>::Iterator::Iterator(const StaticVector<T>::Iterator& iter) : data(iter.data), pos(iter.pos) {}

template <typename T>
const T& StaticVector<T>::Iterator::operator*() {
  return *reinterpret_cast<const T*>(data+pos);
}

template <typename T>
const T* StaticVector<T>::Iterator::operator&() {
  return reinterpret_cast<const T*>(data+pos);
}

template <typename T>
const T* StaticVector<T>::Iterator::operator->() {
  return reinterpret_cast<const T*>(data+pos);
}

template <typename T>
typename StaticVector<T>::Iterator& StaticVector<T>::Iterator::operator++() {
  pos += reinterpret_cast<const T*>(data+pos)->size();
  return *this;
}

template <typename T>
bool StaticVector<T>::Iterator::operator==(const typename StaticVector<T>::Iterator& other) const {
  return pos==other.pos && data==other.data;
}

template <typename T>
bool StaticVector<T>::Iterator::operator!=(const typename StaticVector<T>::Iterator& other) const {
  return pos!=other.pos || data!=other.data;
}

template <typename T>
bool StaticVector<T>::Iterator::operator<(const typename StaticVector<T>::Iterator& other) const {
  return pos<other.pos;
}



template <typename T>
typename StaticVector<T>::Iterator StaticVector<T>::begin() const {
  return Iterator(pool);
}

template <typename T>
typename StaticVector<T>::Iterator StaticVector<T>::end() const {
  return Iterator(pool, size);
}

template <typename T>
typename StaticVector<T>::Iterator StaticVector<T>::find(size_t pos) const {
  return Iterator(pool, pos);
}



#endif
