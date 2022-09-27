#include "statistics_manager.h"

StatisticsManager::StatisticsManager(Dictionary& dict, TripleTable& triple_table) : dict(dict), triple_table(triple_table) {}

StatisticsManager::~StatisticsManager() {}

uint32_t StatisticsManager::getCount(uint32_t subject, uint32_t predicate, uint32_t object) {
  if(subject) {
    if(predicate) {
      if(object) {
        return 1;
      }
      else {
        // sp
        return this->triple_table.count(TripleOrder::SP, subject, predicate, object);
      }
    }
    else {
      if(object) {
        // so
        return 1;
      }
      else {
        // p
        // todo
        return 1;
      }
    }
  }
  else {
    if(predicate) {
      if(object) {
        // op
        return this->triple_table.count(TripleOrder::OP, subject, predicate, object);
      }
      else {
        // p
        return this->triple_table.count(TripleOrder::P, subject, predicate, object);
      }
    }
    else {
      if(object) {
        // o
        // todo
        return 1;
      }
      else {
        return 0;
      }
    }
  }
}

uint32_t StatisticsManager::getCount(TripleOrder key_order, uint32_t subject, uint32_t predicate, uint32_t object) {
  return this->triple_table.count(key_order, subject, predicate, object);
}

uint32_t StatisticsManager::getDistinctCount(TripleOrder key_order, uint32_t subject, uint32_t predicate, uint32_t object) {
  switch(key_order) {
    case SP:
      return this->triple_table.distinctCount(key_order, subject, predicate, object);
    case OP:
      return this->triple_table.distinctCount(key_order, subject, predicate, object);
    case SPO:
      return this->dict.count();
  }
  return 1;
}
