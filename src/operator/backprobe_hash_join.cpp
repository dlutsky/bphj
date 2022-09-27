#include "backprobe_hash_join.h"
#include <xorfilter/xorfilter.h>
#include <xorfilter/hashutil.h>

#include <chrono>
#include <iostream>

BackProbeHashJoin::BackProbeHashJoin(const std::vector<Operator*>& inputs, const std::vector<std::map<uint32_t, Resource*>>& input_resources, const std::map<uint32_t, Resource*>& output_resources, const std::vector<JoinNode>& join_nodes, double expected_cardinality)
  : Operator(expected_cardinality), inputs(inputs), input_resources(input_resources), output_resources(output_resources), bind_resources(input_resources.size()), join_nodes(join_nodes), thld_ratio(0.05), thld_count(10000000) {
}

BackProbeHashJoin::~BackProbeHashJoin() {
  for(int i=0; i<this->hash_tables.size(); ++i) {
    delete this->hash_tables[i];
  }
}

void BackProbeHashJoin::open() {
  for(int i=0; i<inputs.size(); i++) {
    inputs[i]->open();
  }
  /*
  for(int i=0; i<join_nodes.size(); ++i) {
    std::cout<<join_nodes[i].left_join_key<<" "<<join_nodes[i].right_join_key<<std::endl;
  }
  */

  std::set<uint32_t> checked;
  uint32_t join_res = UINT32_MAX;
  for(int i=input_resources.size()-1; i>=0; i--) {
    for(std::map<uint32_t, Resource*>::iterator iter = input_resources[i].begin(), end = input_resources[i].end(); iter != end; ++iter) {
      if(checked.count(iter->first) != 0) {
        continue;
      }

      if(output_resources.count(iter->first) != 0) {
        bind_resources[i].push_back(std::make_pair(iter->second, output_resources[iter->first]));
        checked.insert(iter->first);
      }
    }
  }
}

void BackProbeHashJoin::close() {
  for(int i=0; i<inputs.size(); i++) {
    this->inputs[i]->close();
  }
}

bool BackProbeHashJoin::first() {
  // build
  build();

  // proble
  //auto start = std::chrono::high_resolution_clock::now();
  probe();
  /*
  auto done = std::chrono::high_resolution_clock::now();
  double exec_time = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(done-start).count();
  std::cout << std::endl << "probe time: " << exec_time << " ms" << std::endl;
  */
  return true;
}

bool BackProbeHashJoin::next() {
  return false;
}

void BackProbeHashJoin::build() {
  for(int i=0; i<this->join_nodes.size(); i++) {
    // auto start = std::chrono::high_resolution_clock::now();

    bool only_count = false;
    /*
    if(this->input_resources[i].size() == 1) {
      only_count = true;
    }
    */
    const JoinNode& node = this->join_nodes[i];
    if(node.left_join_key != UINT32_MAX && node.left_join_key != node.right_join_key) {
      if(node.can_filter && (this->hash_table_map.count(node.right_join_key)?this->hash_table_map[node.left_join_key]->numOfKeys()<this->hash_table_map[node.right_join_key]->numOfKeys():true) && (double)this->hash_table_map[node.left_join_key]->numOfKeys() / this->inputs[i]->getExpectedCardinality() < thld_ratio) {
        if(node.left_res_pos == ResourcePosition::SUBJECT) {
          this->inputs[i]->setTripleOrder(TripleOrder::SP);
        } else {
          this->inputs[i]->setTripleOrder(TripleOrder::OP);
        }
        Resource* left_res = input_resources[i][node.left_join_key];
        Resource* right_res = input_resources[i][node.right_join_key];

        this->hash_table_map[node.left_join_key]->getAllKeys(left_res->column);

        HashTable* hash_table;
        if(!this->hash_table_map.count(node.right_join_key)) {
          hash_table = new HashTable(node.right_join_key);
          this->hash_tables.push_back(hash_table);
          this->hash_table_map[node.right_join_key] = hash_table;
          hash_table->init(i);
          int j=0;
          if(!only_count) {
            if(this->inputs[i]->first()) {
              for(; j<right_res->column.size(); j++) {
                hash_table->insert(right_res->column[j], j);
              }
              while(this->inputs[i]->next()) {
                for(; j<right_res->column.size(); j++) {
                  hash_table->insert(right_res->column[j], j);
                }
              }
            }
          } else {
            if(this->inputs[i]->first()) {
              for(; j<right_res->column.size(); j++) {
                hash_table->insert(right_res->column[j]);
              }
              while(this->inputs[i]->next()) {
                for(; j<right_res->column.size(); j++) {
                  hash_table->insert(right_res->column[j]);
                }
              }
            }
          }
        } else {
          hash_table = this->hash_table_map[node.right_join_key];
          hash_table->init(i);
          int j=0;
          if(!only_count) {
            if(this->inputs[i]->first()) {
              for(; j<right_res->column.size(); j++) {
                hash_table->append(right_res->column[j], j);
              }
              while(this->inputs[i]->next()) {
                for(; j<right_res->column.size(); j++) {
                  hash_table->append(right_res->column[j], j);
                }
              }
            }
          } else {
            if(this->inputs[i]->first()) {
              for(; j<right_res->column.size(); j++) {
                hash_table->append(right_res->column[j]);
              }
              while(this->inputs[i]->next()) {
                for(; j<right_res->column.size(); j++) {
                  hash_table->append(right_res->column[j]);
                }
              }
            }
          }
        }
      } else {
        Resource* left_res = input_resources[i][node.left_join_key];
        Resource* right_res = input_resources[i][node.right_join_key];

        if(node.can_filter && this->hash_table_map.count(node.right_join_key)) {
          if((double)this->hash_table_map[node.right_join_key]->numOfKeys() / this->inputs[i]->getExpectedCardinality() < thld_ratio) {
            if(node.right_res_pos == ResourcePosition::SUBJECT) {
              this->inputs[i]->setTripleOrder(TripleOrder::SP);
            } else {
              this->inputs[i]->setTripleOrder(TripleOrder::OP);
            }
            this->hash_table_map[node.right_join_key]->getAllKeys(right_res->column);
          }
        }

        HashTable* filter_hash_table = this->hash_table_map[node.left_join_key];
        HashTable* hash_table;

        if(!this->hash_table_map.count(node.right_join_key)) {
          hash_table = new HashTable(node.right_join_key);
          this->hash_tables.push_back(hash_table);
          this->hash_table_map[node.right_join_key] = hash_table;
          hash_table->init(i);
          int j=0;
          if(!only_count) {
            if(this->inputs[i]->first()) {
              for(; j<right_res->column.size(); j++) {
                if(filter_hash_table->exist(left_res->column[j])) {
                  hash_table->insert(right_res->column[j], j);
                }
              }
              while(this->inputs[i]->next()) {
                for(; j<right_res->column.size(); j++) {
                  if(filter_hash_table->exist(left_res->column[j])) {
                    hash_table->insert(right_res->column[j], j);
                  }
                }
              }
            }
          } else {
            if(this->inputs[i]->first()) {
              for(; j<right_res->column.size(); j++) {
                if(filter_hash_table->exist(left_res->column[j])) {
                  hash_table->insert(right_res->column[j]);
                }
              }
              while(this->inputs[i]->next()) {
                for(; j<right_res->column.size(); j++) {
                  if(filter_hash_table->exist(left_res->column[j])) {
                    hash_table->insert(right_res->column[j]);
                  }
                }
              }
            }
          }
        } else {
          hash_table = this->hash_table_map[node.right_join_key];
          hash_table->init(i);
          int j=0;
          if(!only_count) {
            if(this->inputs[i]->first()) {
              for(; j<right_res->column.size(); j++) {
                if(filter_hash_table->exist(left_res->column[j])) {
                  hash_table->append(right_res->column[j], j);
                }
              }
              while(this->inputs[i]->next()) {
                for(; j<right_res->column.size(); j++) {
                  if(filter_hash_table->exist(left_res->column[j])) {
                    hash_table->append(right_res->column[j], j);
                  }
                }
              }
            }
          } else {
            if(this->inputs[i]->first()) {
              for(; j<right_res->column.size(); j++) {
                if(filter_hash_table->exist(left_res->column[j])) {
                  hash_table->append(right_res->column[j]);
                }
              }
              while(this->inputs[i]->next()) {
                for(; j<right_res->column.size(); j++) {
                  if(filter_hash_table->exist(left_res->column[j])) {
                    hash_table->append(right_res->column[j]);
                  }
                }
              }
            }
          }
        }
      }
    } else {
      Resource* right_res = input_resources[i][node.right_join_key];

      if(node.can_filter && this->hash_table_map.count(node.right_join_key)) {
        if((double)this->hash_table_map[node.right_join_key]->numOfKeys() / this->inputs[i]->getExpectedCardinality() < thld_ratio) {
          if(node.right_res_pos == ResourcePosition::SUBJECT) {
            this->inputs[i]->setTripleOrder(TripleOrder::SP);
          } else {
            this->inputs[i]->setTripleOrder(TripleOrder::OP);
          }
          this->hash_table_map[node.right_join_key]->getAllKeys(right_res->column);
        }
      }


      HashTable* hash_table;
      if(!this->hash_table_map.count(node.right_join_key)) {
        hash_table = new HashTable(node.right_join_key);
        this->hash_tables.push_back(hash_table);
        this->hash_table_map[node.right_join_key] = hash_table;
        hash_table->init(i);
        int j=0;
        if(!only_count) {
          if(this->inputs[i]->first()) {
            for(; j<right_res->column.size(); j++) {
              hash_table->insert(right_res->column[j], j);
            }
            while(this->inputs[i]->next()) {
              for(; j<right_res->column.size(); j++) {
                hash_table->insert(right_res->column[j], j);
              }
            }
          }
        } else {
          if(this->inputs[i]->first()) {
            for(; j<right_res->column.size(); j++) {
              hash_table->insert(right_res->column[j]);
            }
            while(this->inputs[i]->next()) {
              for(; j<right_res->column.size(); j++) {
                hash_table->insert(right_res->column[j]);
              }
            }
          }
        }
      } else {
        hash_table = this->hash_table_map[node.right_join_key];
        hash_table->init(i);
        int j=0;
        if(!only_count) {
          if(this->inputs[i]->first()) {
            for(; j<right_res->column.size(); j++) {
              hash_table->append(right_res->column[j], j);
            }
            while(this->inputs[i]->next()) {
              for(; j<right_res->column.size(); j++) {
                hash_table->append(right_res->column[j], j);
              }
            }
          }
        } else {
          if(this->inputs[i]->first()) {
            for(; j<right_res->column.size(); j++) {
              hash_table->append(right_res->column[j]);
            }
            while(this->inputs[i]->next()) {
              for(; j<right_res->column.size(); j++) {
                hash_table->append(right_res->column[j]);
              }
            }
          }
        }
      }
    }
  }
}

void BackProbeHashJoin::probe() {
  int k = this->hash_tables.size()-1;
  HashTable* hash_table = this->hash_tables[k];
  HashTable::Iterator iter = hash_table->begin();
  HashTable::Iterator end = hash_table->end();
  for(; iter != end; ++iter) {
    Entry* entry = &iter->entry;
    EntryHead* next_head = iter->next;
    int i = hash_table->getLevelValue(iter->level);
    while(entry != nullptr) {
      int idx = entry->value;
      for(int j=0; j<bind_resources[i].size(); j++) {
        bind_resources[i][j].second->id = bind_resources[i][j].first->column[idx];
      }
      enumerate(hash_table, k-1, next_head);
      entry = entry->next;
    }
  }
}

void BackProbeHashJoin::enumerate(HashTable* hash_table, int k, EntryHead* head) {
  if(head != nullptr) {
    Entry* entry = &head->entry;
    EntryHead* next_head = head->next;
    int i = hash_table->getLevelValue(head->level);
    while(entry != nullptr) {
      int idx = entry->value;
      for(int j=0; j<bind_resources[i].size(); ++j) {
        bind_resources[i][j].second->id = bind_resources[i][j].first->column[idx];
      }
      enumerate(hash_table, k, next_head);
      entry = entry->next;
    }
    return;
  }
  if(k >= 0) {
    HashTable* new_hash_table = this->hash_tables[k];
    EntryHead* new_head = new_hash_table->lookup(output_resources[new_hash_table->getKeyResourceId()]->id);
    if(new_head == nullptr) {
      return;
    }
    Entry* entry = &new_head->entry;
    EntryHead* next_head = new_head->next;
    int i = new_hash_table->getLevelValue(new_head->level);
    while(entry != nullptr) {
      int idx = entry->value;
      for(int j=0; j<bind_resources[i].size(); j++) {
        bind_resources[i][j].second->id = bind_resources[i][j].first->column[idx];
      }
      enumerate(new_hash_table, k-1, next_head);
      entry = entry->next;
    }
  } else {
    for(std::map<uint32_t, Resource*>::iterator iter = output_resources.begin(), end = output_resources.end(); iter != end; ++iter) {
      iter->second->column.push_back(iter->second->id);
    }
  }
}

// HashIterator
BackProbeHashJoin::HashTable::HashTable(uint32_t key_res) : key_res(key_res), num_keys(0), curr_level(-1), prev_level(-1) {
}

void BackProbeHashJoin::HashTable::init(int level_val) {
  this->prev_level = this->curr_level;
  ++this->curr_level;
  this->num_keys = 0;
  this->level_vals.push_back(level_val);
}

void BackProbeHashJoin::HashTable::insert(uint32_t key) {
  auto it = this->table.find(key);
  if(it != this->table.end()) {
    EntryHead* head = it->second;
    ++head->entry.count;
  } else {
    ++num_keys;
    EntryHead* head = head_pool.alloc();
    head->next = nullptr;
    head->level = curr_level;
    head->entry.count = 1;
    head->entry.next = nullptr;
    table[key] = head;
  }
}

void BackProbeHashJoin::HashTable::insert(uint32_t key, uint32_t value) {
  auto it = this->table.find(key);
  if(it != this->table.end()) {
    EntryHead* head = it->second;
    Entry* prev = &head->entry;

    Entry* entry = entry_pool.alloc();
    entry->value = value;
    entry->count = 1;
    entry->next = prev->next;
    prev->next = entry;
  } else {
    ++num_keys;
    EntryHead* head = head_pool.alloc();
    head->next = nullptr;
    head->level = curr_level;
    head->entry.value = value;
    head->entry.count = 1;
    head->entry.next = nullptr;
    table[key] = head;
  }
}

void BackProbeHashJoin::HashTable::append(uint32_t key) {
  auto it = this->table.find(key);
  if(it != this->table.end()) {
    EntryHead* head = it->second;
    if(curr_level == head->level) {
      ++head->entry.count;
    } else if(prev_level == head->level) {
      ++num_keys;
      EntryHead* new_head = head_pool.alloc();
      new_head->next = head;
      new_head->level = curr_level;
      new_head->entry.count = 1;
      new_head->entry.next = nullptr;
      table[key] = new_head;
    }
  }
}

void BackProbeHashJoin::HashTable::append(uint32_t key, uint32_t value) {
  auto it = this->table.find(key);
  if(it != this->table.end()) {
    EntryHead* head = it->second;
    if(curr_level == head->level) {
      Entry* prev = &head->entry;

      Entry* entry = entry_pool.alloc();
      entry->value = value;
      entry->count = 1;
      entry->next = prev->next;
      prev->next = entry;
    } else if(prev_level == head->level) {
      ++num_keys;
      EntryHead* new_head = head_pool.alloc();
      new_head->next = head;
      new_head->level = curr_level;
      new_head->entry.value = value;
      new_head->entry.count = 1;
      new_head->entry.next = nullptr;
      table[key] = new_head;
    }
  }
}

BackProbeHashJoin::EntryHead* BackProbeHashJoin::HashTable::lookup(uint32_t key) {
  Entry* entry;
  auto it = this->table.find(key);
  if(it != this->table.end()) {
    if(it->second->level == curr_level) {
      return it->second;
    }
  }
  return nullptr;
}

bool BackProbeHashJoin::HashTable::exist(uint32_t key) {
  auto it = this->table.find(key);
  return it != this->table.end() && it->second->level == curr_level;
}

uint32_t BackProbeHashJoin::HashTable::getKeyResourceId() {
  return this->key_res;
}

int BackProbeHashJoin::HashTable::getLevelValue(int level) {
  return this->level_vals[level];
}

int BackProbeHashJoin::HashTable::numOfKeys() {
  return num_keys;
}

bool BackProbeHashJoin::HashTable::getAllKeys(std::vector<uint32_t>& keys) {
  for(ska::flat_hash_map<uint32_t, EntryHead*>::iterator it = this->table.begin(), end = this->table.end(); it != end; ++it) {
    if(it->second->level == curr_level) {
      keys.push_back(it->first);
    }
  }
}

BackProbeHashJoin::HashTable::Iterator::Iterator(ska::flat_hash_map<uint32_t, EntryHead*>::iterator iter, ska::flat_hash_map<uint32_t, EntryHead*>::iterator end, int curr_level)
  : iter(iter), end(end), curr_level(curr_level) {
  while(this->iter != end && this->iter->second->level != curr_level) {
    ++this->iter;
  }
}

BackProbeHashJoin::EntryHead& BackProbeHashJoin::HashTable::Iterator::operator*() {
  return *iter->second;
}
BackProbeHashJoin::EntryHead* BackProbeHashJoin::HashTable::Iterator::operator->() {
  return iter->second;
}

BackProbeHashJoin::HashTable::Iterator& BackProbeHashJoin::HashTable::Iterator::operator++() {
  ++iter;
  while(iter != end && iter->second->level != curr_level) {
    ++iter;
  }
  return *this;
}

bool BackProbeHashJoin::HashTable::Iterator::operator!=(const BackProbeHashJoin::HashTable::Iterator& other) const {
  return this->iter != other.iter;
}

BackProbeHashJoin::HashTable::Iterator BackProbeHashJoin::HashTable::begin() {
  return Iterator(table.begin(), table.end(), curr_level);
}

BackProbeHashJoin::HashTable::Iterator BackProbeHashJoin::HashTable::end() {
  return Iterator(table.end(), table.end(), curr_level);
}
