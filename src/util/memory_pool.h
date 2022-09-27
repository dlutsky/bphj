#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <cstdint>
#include <cstdio>
#include <cstring>


template <typename T>
class MemoryPool {
public:
  MemoryPool();
  MemoryPool(int item_size);
  ~MemoryPool();

  T* alloc();
  void free(T* item);
  void clear();

private:
  const static uint32_t BLOCK_CAPACITY;

  struct Block {
    Block* next;
    char data[1];
  };

  Block* head;
  T* free_list;

  int block_capacity;
  int block_size;
  int item_size;
};


template <typename T>
MemoryPool<T>::MemoryPool() : head(0), free_list(0), block_capacity(BLOCK_CAPACITY) {
  this->item_size = sizeof(T) < sizeof(T*) ? sizeof(T*) : sizeof(T);
  this->block_size = sizeof(Block*) + this->item_size * this->block_capacity;
}

template <typename T>
MemoryPool<T>::MemoryPool(int item_size) : head(0), free_list(0), block_capacity(BLOCK_CAPACITY) {
  this->item_size = item_size < sizeof(T*) ? sizeof(T*) : item_size;
  this->block_size = sizeof(Block*) + this->item_size * this->block_capacity;
}

template <typename T>
MemoryPool<T>::~MemoryPool() {
  clear();
}

template <typename T>
const uint32_t MemoryPool<T>::BLOCK_CAPACITY = 32;

template <typename T>
T* MemoryPool<T>::alloc() {
  if(!free_list) {
    Block* block = reinterpret_cast<Block*>(new char[block_size]);
    block->next = this->head;
    this->head = block;

    char* data = block->data + this->item_size;
    for(int i = 1; i < block_capacity; ++i) {
      *reinterpret_cast<T**>(data - this->item_size) = reinterpret_cast<T*>(data);
      data = data + this->item_size;
    }
    *(reinterpret_cast<T**>(data - this->item_size)) = 0;
    free_list = reinterpret_cast<T*>(block->data);
  }

  T* item = free_list;
  free_list = *reinterpret_cast<T**>(item);
  std::memset(item, 0, this->item_size);
  return item;
}

template <typename T>
void MemoryPool<T>::free(T* item) {
  *reinterpret_cast<T**>(item) = free_list;
  free_list = item;
}

template <typename T>
void MemoryPool<T>::clear() {
  Block* block;
  while(this->head) {
    block = this->head->next;
    delete[] reinterpret_cast<char*>(this->head);
    this->head = block;
  }
  head = 0;
  free_list = 0;
}


#endif
