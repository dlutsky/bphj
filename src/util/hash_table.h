#ifndef HASH_TABLE_H
#define HASH_TABLE_H


#include <vector>
#include <list>
#include <sys/types.h>


template <typename K, typename V, typename H>
class HashTable {
private:
  struct Bucket {
    V value;
    Bucket* next;
    Bucket(V value, Bucket* next) : value(value), next(next) {}
  };
  struct Entry {
    K key;
    int val_num;
    Bucket* next;
    Entry(K key, int val_num, Bucket* next) : key(key), val_num(val_num), next(next) {}
  };

public:
  class ValueIterator {
  public:
    ValueIterator();
    ValueIterator(Bucket* bucket);
    ValueIterator(const ValueIterator& iter);
    V& operator*();
    V* operator->();
    ValueIterator& operator++();
    bool valid();

  private:
    Bucket* curr_bucket;
  };

  HashTable();
  HashTable(size_t size);

  ValueIterator lookup(const K& key);
  bool insert(const K& key, const V& value);

private:

  Entry* search(const K& key);
  bool insert(Entry* entry);
  void rehash();

  std::list<Bucket> buckets;
  std::list<Entry> container;
  std::vector<Entry*> table;
  size_t table_size;
  int max_loop;
};



template <typename K, typename V, typename H>
HashTable<K, V, H>::ValueIterator::ValueIterator() : curr_bucket(nullptr) {}

template <typename K, typename V, typename H>
HashTable<K, V, H>::ValueIterator::ValueIterator(HashTable<K, V, H>::Bucket* bucket) : curr_bucket(bucket) {}

template <typename K, typename V, typename H>
HashTable<K, V, H>::ValueIterator::ValueIterator(const HashTable<K, V, H>::ValueIterator& iter) : curr_bucket(iter.curr_bucket) {}

template <typename K, typename V, typename H>
V& HashTable<K, V, H>::ValueIterator::operator*() {
  return curr_bucket->value;
}

template <typename K, typename V, typename H>
V* HashTable<K, V, H>::ValueIterator::operator->() {
  return &(curr_bucket->value);
}

template <typename K, typename V, typename H>
typename HashTable<K, V, H>::ValueIterator& HashTable<K, V, H>::ValueIterator::operator++() {
  curr_bucket = curr_bucket->next;
  return *this;
}

template <typename K, typename V, typename H>
bool HashTable<K, V, H>::ValueIterator::valid() {
  if(curr_bucket) {
    return true;
  }
  return false;
}



template <typename K, typename V, typename H>
HashTable<K, V, H>::HashTable() : table_size(1024), max_loop(512) {
  table.resize(table_size*2);
}

template <typename K, typename V, typename H>
HashTable<K, V, H>::HashTable(size_t size) {
  table_size = 2;
  while(table_size < size) {
    table_size <<= 1;
  }
  max_loop = table_size >> 1;
  table.resize(table_size*2);
}

template <typename K, typename V, typename H>
typename HashTable<K, V, H>::ValueIterator HashTable<K, V, H>::lookup(const K& key) {
  Entry* entry = search(key);
  if(entry) {
    return ValueIterator(entry->next);
  }
  return ValueIterator();
}

template <typename K, typename V, typename H>
bool HashTable<K, V, H>::insert(const K& key, const V& value) {
  Entry* entry = search(key);
  if(entry) {
    entry->val_num++;
    buckets.push_back(Bucket(value, entry->next));
    entry->next = &buckets.back();
    return true;
  }
  buckets.push_back(Bucket(value, nullptr));
  container.push_back(Entry(key, 1, &buckets.back()));
  return insert(&container.back());
}

template <typename K, typename V, typename H>
typename HashTable<K, V, H>::Entry* HashTable<K, V, H>::search(const K& key) {
  Entry* entry;
  entry = table[H::hash1(key, table_size)];
  if(entry && entry->key == key) {
    return entry;
  }
  entry = table[table_size + H::hash2(key, table_size)];
  if(entry && entry->key == key) {
    return entry;
  }
  return nullptr;
}

template <typename K, typename V, typename H>
bool HashTable<K, V, H>::insert(HashTable<K, V, H>::Entry* entry) {
  for(int i = 0; i < max_loop; ++i) {
    std::swap(entry, table[H::hash1(entry->key, table_size)]);
    if(!entry) {
      return true;
    }
    std::swap(entry, table[table_size + H::hash2(entry->key, table_size)]);
    if(!entry) {
      return true;
    }
  }
  rehash();
  insert(entry);
}

template <typename K, typename V, typename H>
void HashTable<K, V, H>::rehash() {
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




#endif
