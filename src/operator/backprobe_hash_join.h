#ifndef BACKPROBE_HASH_JOIN_H
#define BACKPROBE_HASH_JOIN_H

#include <utility>
#include <vector>
#include <map>
#include <unordered_map>
#include <ska/flat_hash_map.hpp>
#include "operator.h"
#include "util/memory_pool.h"
#include "plan/query_plan.h"


class BackProbeHashJoin : public Operator {
public:
  BackProbeHashJoin(const std::vector<Operator*>& inputs, const std::vector<std::map<uint32_t, Resource*>>& input_resources, const std::map<uint32_t, Resource*>& output_resources, const std::vector<JoinNode>& join_nodes, double expected_cardinality);
  ~BackProbeHashJoin();

  void open();
  void close();

  void consume(int idx);
  void produce(Operator* parent, int idx);

  bool first();
  bool next();

protected:
  struct Entry {
    uint32_t value;
    uint32_t count;
    Entry* next;
  };
  struct EntryHead {
    int level;
    Entry entry;
    EntryHead* next;
  };
  class HashTable {
  public:
    HashTable(uint32_t key_res);

    void init(int level_val);
    void insert(uint32_t key);
    void insert(uint32_t key, uint32_t value);
    void append(uint32_t key);
    void append(uint32_t key, uint32_t value);
    EntryHead* lookup(uint32_t key);
    bool exist(uint32_t key);

    uint32_t getKeyResourceId();
    int getLevelValue(int level);
    int numOfKeys();
    bool getAllKeys(std::vector<uint32_t>& keys);

    class Iterator {
    public:
      Iterator(ska::flat_hash_map<uint32_t, EntryHead*>::iterator iter, ska::flat_hash_map<uint32_t, EntryHead*>::iterator end, int curr_level);
      EntryHead& operator*();
      EntryHead* operator->();
      Iterator& operator++();
      bool operator!=(const Iterator& other) const;

    private:
      ska::flat_hash_map<uint32_t, EntryHead*>::iterator iter;
      ska::flat_hash_map<uint32_t, EntryHead*>::iterator end;
      int curr_level;
    };

    Iterator begin();
    Iterator end();

  private:
    uint32_t key_res;
    ska::flat_hash_map<uint32_t, EntryHead*> table;
    MemoryPool<EntryHead> head_pool;
    MemoryPool<Entry> entry_pool;
    std::vector<int> level_vals;
    int num_keys;
    int prev_level;
    int curr_level;
  };

  void build();
  void probe();
  void enumerate(HashTable* hash_table, int k, EntryHead* head);

  double thld_ratio;
  int thld_count;

  std::vector<Operator*> inputs;
  std::vector<std::map<uint32_t, Resource*>> input_resources;
  std::map<uint32_t, Resource*> output_resources;

  std::vector<std::vector<std::pair<Resource*, Resource*>>> bind_resources;

  std::vector<JoinNode> join_nodes;

  std::map<uint32_t, HashTable*> hash_table_map;
  std::vector<HashTable*> hash_tables;
};






#endif
