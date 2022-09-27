#include "hash_join.h"

HashJoin::HashJoin(Operator* left, const std::vector<Resource*>& left_resources, Operator* right, const std::vector<Resource*>& right_resources, double expected_cardinality)
  : Operator(expected_cardinality), left(left), left_resources(left_resources), right(right), right_resources(right_resources) {
}

void HashJoin::open() {
  left->open();
  right->open();
  /*
  HashTableBuilder builder(*this);
  Thread thread(&builder);
  thread->start();
  */
}

void HashJoin::close() {
  left->close();
  right->close();
}


bool HashJoin::first() {
  return true;
}

bool HashJoin::next() {
  return true;
}




HashJoin::HashTable::HashTable() : table_size(1024), max_loop(512), num_keys(0) {
  table.resize(table_size*2);
}

HashJoin::HashTable::HashTable(size_t size) {
  table_size = 2;
  while(table_size < size) {
    table_size <<= 1;
  }
  max_loop = table_size >> 1;
  table.resize(table_size*2);
}

size_t hash1 (uint32_t key, size_t table_size) {
  return key & (table_size-1);
}

size_t hash2 (uint32_t key, size_t table_size) {
  return (key << 32) & (table_size-1);
}

void HashJoin::HashTable::insert(const std::vector<Resource*>& resources) {
  Entry* entry = pool.alloc();
  entry->key = resources[0]->id;
  for(int i=0, j=1; j<resources.size(); i++, j++) {
    entry->values[i] = resources[j]->id;
  }
  entry->next = nullptr;

  Entry* prev = lookup(entry->key);
  if(prev) {
    entry->next = prev->next;
    prev->next = entry;
  } else {
    ++num_keys;
    insert(entry);
  }
}

HashJoin::Entry* HashJoin::HashTable::lookup(uint32_t key) {
  Entry* entry;
  entry = this->table[hash1(key, table_size)];
  if(entry && entry->key == key) {
    return entry;
  }
  entry = this->table[table_size + hash2(key, table_size)];
  if(entry && entry->key == key) {
    return entry;
  }
  return nullptr;
}

bool HashJoin::HashTable::exist(uint32_t key) {
  Entry* entry;
  entry = this->table[hash1(key, table_size)];
  if(entry && entry->key == key) {
    return true;
  }
  entry = this->table[table_size + hash2(key, table_size)];
  if(entry && entry->key == key) {
    return true;
  }
  return false;
}

int HashJoin::HashTable::numOfKeys() {
  return num_keys;
}

bool HashJoin::HashTable::getAllKeys(std::vector<uint32_t>& keys) {
  for(int i=0; i<this->table.size(); i++) {
    Entry* entry = this->table[i];
    if(entry) {
      keys.push_back(entry->key);
    }
  }
}

void HashJoin::HashTable::insert(Entry* entry) {
  for(int i = 0; i < max_loop; ++i) {
    std::swap(entry, table[hash1(entry->key, table_size)]);
    if(!entry) {
      return;
    }
    std::swap(entry, table[table_size + hash2(entry->key, table_size)]);
    if(!entry) {
      return;
    }
  }
  rehash();
  insert(entry);
}

void HashJoin::HashTable::rehash() {
  table_size *= 2;
  std::vector<Entry*> old_table;
  old_table.resize(table_size*2);
  swap(table, old_table);
  for (typename std::vector<Entry*>::const_iterator iter=old_table.begin(), end=old_table.end(); iter!=end; ++iter) {
    if (*iter) {
      insert(*iter);
    }
  }
}




HashJoin::HashTableBuilder::HashTableBuilder(HashJoin& join) : join(join) {}

void HashJoin::HashTableBuilder::run() {
}
