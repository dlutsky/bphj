#ifndef LRU_CACHE_H
#define LRU_CACHE_H

#include <list>
#include <map>
#include <utility>


template <typename K, typename V>
class LRUCache {
public:
  LRUCache(int capacity);
  V get(K key);
  void set(K key, V value);

private:
  int capacity;
  std::list<std::pair<K, V>> cache;
  typedef typename std::list<std::pair<K, V>>::iterator CacheIterator;
  std::map<K, CacheIterator> cacheLoc;
};




template <typename K, typename V>
LRUCache<K, V>::LRUCache(int capacity) : capacity(capacity) {}

template <typename K, typename V>
V LRUCache<K, V>::get(K key) {
  if(cacheLoc.count(key)){
    CacheIterator iter = cacheLoc[key];
    cache.splice(cache.begin(), cache, iter);
    return iter->second;
  }
  else{
    return nullptr;
  }
}

template <typename K, typename V>
void LRUCache<K, V>::set(K key, V value) {
  if(cacheLoc.count(key)){
    CacheIterator iter = cacheLoc[key];
    cache.splice(cache.begin(), cache, iter);
    iter->second = value;
    return;
  }
  if(cache.size() == capacity){
    K k=cache.back().first;
    cache.pop_back();
    cacheLoc.erase(k);
  }
  cache.push_front(std::make_pair(key, value));
  cacheLoc[key] = cache.begin();
}




#endif
