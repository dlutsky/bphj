#ifndef HASH_JOIN_H
#define HASH_JOIN_H

#include <vector>
#include "operator.h"
#include "util/memory_pool.h"
#include "thread/runnable.h"


class HashJoin : public Operator {
public:
  HashJoin(Operator* left, const std::vector<Resource*>& left_resources, Operator* right, const std::vector<Resource*>& right_resources, double expected_cardinality);
  ~HashJoin();

  void open();
  void close();

  bool first();
  bool next();

protected:
  struct Entry {
    Entry* next;
    uint32_t key;
    uint32_t values[];
    Entry(uint32_t key) : next(nullptr), key(key) {}
  };
  class HashTable {
  public:
    HashTable();
    HashTable(size_t size);

    void insert(const std::vector<Resource*>& resources);
    Entry* lookup(uint32_t key);
    bool exist(uint32_t key);

    int numOfKeys();
    bool getAllKeys(std::vector<uint32_t>& keys);
  private:
    void insert(Entry* entry);
    void rehash();

    MemoryPool<Entry> pool;
    std::vector<Entry*> table;
    size_t table_size;
    int max_loop;
    int num_keys;
  };
  class HashTableBuilder : public Runnable {
  public:
    HashTableBuilder(HashJoin& join);
    void run();
  private:
    HashJoin& join;
  };
  friend class HashTableBuilder;

  Operator *left, *right;
  std::vector<Resource*> left_resources, right_resources;

  HashTable hash_table;
};


#endif
