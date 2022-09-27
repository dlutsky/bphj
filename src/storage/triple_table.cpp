#include <cstring>
#include "triple_table.h"

#include <iostream>

TripleTable::TripleTable(const std::string& store_path, const std::string& table_name, BufferManager* buffer_manager)
  : kvstore(store_path + "/" + table_name), data_file(store_path + "/" + table_name + "_data.graw", buffer_manager), index_file(store_path + "/" + table_name + "_index.graw", buffer_manager) {}

TripleTable::~TripleTable() {}

bool TripleTable::open() {
  if(!this->kvstore.open()) {
    return false;
  }

  if(!this->data_file.open()) {
    return false;
  }

  if(!this->index_file.open()) {
    return false;
  }
  return true;
}

bool TripleTable::close() {
  if(!this->data_file.close()) {
    return false;
  }

  if(!this->index_file.close()) {
    return false;
  }

  if(!this->kvstore.close()) {
    return false;
  }
  return true;
}

TripleTable::BlockScanner::BlockScanner(TripleTable& table, TripleOrder key_order, Resource *subject, Resource *predicate, Resource *object)
  : table(table), key_order(key_order), subject(subject), predicate(predicate), object(object), idx(0), x_block_no(0), y_block_no(0) {
}

bool TripleTable::BlockScanner::find() {
  switch(key_order) {
    case P:
      {
        Key key = { 1, predicate->id, 0};
        std::string value;
        if(!this->table.kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          return false;
        }
        values.push_back(value);
        return true;
      }
      break;
    case SP:
      {
        if(subject->column.size() > 0) {
          key_ids = subject->column;
          std::vector<std::string> keys(key_ids.size());
          Key key = { 2, 0, predicate->id};
          for(int i=0; i<key_ids.size(); i++) {
            key.x = key_ids[i];
            keys[i] = std::string(reinterpret_cast<char*>(&key), KEY_SIZE);
          }
          if(!this->table.kvstore.multiGet(keys, &values)){
            return false;
          }
          subject->column.clear();
        } else {
          Key key = { 2, subject->id, predicate->id};
          std::string value;
          if(!this->table.kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
            return false;
          }
          key_ids.push_back(subject->id);
          values.push_back(value);
        }
        return true;
      }
      break;
    case OP:
      {
        if(object->column.size() > 0) {
          key_ids = object->column;
          std::vector<std::string> keys(key_ids.size());
          Key key = { 4, 0, predicate->id};
          for(int i=0; i<key_ids.size(); i++) {
            key.x = key_ids[i];
            keys[i] = std::string(reinterpret_cast<char*>(&key), KEY_SIZE);
          }
          if(!this->table.kvstore.multiGet(keys, &values)){
            return false;
          }
          object->column.clear();
        } else {
          Key key = { 4, object->id, predicate->id};
          std::string value;
          if(!this->table.kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
            return false;
          }
          key_ids.push_back(object->id);
          values.push_back(value);
        }
        return true;
      }
      break;
  }
  return false;
}

bool TripleTable::BlockScanner::read() {
  if(x_block_no != 0 && y_block_no != 0) {
    BufferPage* page = this->table.data_file.getNode(x_block_no);
    char* block = page->getBlockData();
    Node* node = reinterpret_cast<Node*>(block);

    subject->column.insert(subject->column.end(), reinterpret_cast<uint32_t*>(node->data), reinterpret_cast<uint32_t*>(node->data)+node->dsize/sizeof(uint32_t));

    x_block_no = node->next_block_no;
    this->table.data_file.updateNode(page, false, true);

    page = this->table.data_file.getNode(y_block_no);
    block = page->getBlockData();
    node = reinterpret_cast<Node*>(block);

    object->column.insert(object->column.end(), reinterpret_cast<uint32_t*>(node->data), reinterpret_cast<uint32_t*>(node->data)+node->dsize/sizeof(uint32_t));

    y_block_no = node->next_block_no;
    this->table.data_file.updateNode(page, false, true);

    // Prefetch next nodes
    if(x_block_no != 0) {
      this->table.data_file.prefetchNode(x_block_no);
    }
    if(y_block_no != 0) {
      this->table.data_file.prefetchNode(y_block_no);
    }
    return true;
  } else if(x_block_no != 0) {
    BufferPage* page = this->table.data_file.getNode(x_block_no);
    char* block = page->getBlockData();
    Node* node = reinterpret_cast<Node*>(block);

    subject->column.insert(subject->column.end(), reinterpret_cast<uint32_t*>(node->data), reinterpret_cast<uint32_t*>(node->data)+node->dsize/sizeof(uint32_t));
    if(object != nullptr) {
      for(int k=object->column.size(); k<subject->column.size(); k++) {
        object->column.push_back(object->id);
      }
    }

    x_block_no = node->next_block_no;
    this->table.data_file.updateNode(page, false, true);

    // Prefetch next nodes
    if(x_block_no != 0) {
      this->table.data_file.prefetchNode(x_block_no);
    }
    return true;
  } else if(y_block_no != 0) {
    BufferPage* page = this->table.data_file.getNode(y_block_no);
    char* block = page->getBlockData();
    Node* node = reinterpret_cast<Node*>(block);

    object->column.insert(object->column.end(), reinterpret_cast<uint32_t*>(node->data), reinterpret_cast<uint32_t*>(node->data)+node->dsize/sizeof(uint32_t));
    if(subject != nullptr) {
      for(int k=subject->column.size(); k<object->column.size(); k++) {
        subject->column.push_back(subject->id);
      }
    }

    y_block_no = node->next_block_no;
    this->table.data_file.updateNode(page, false, true);

    // Prefetch next nodes
    if(y_block_no != 0) {
      this->table.data_file.prefetchNode(y_block_no);
    }
    return true;
  }
  return false;
}

bool TripleTable::BlockScanner::next() {
  if(read()) {
    return true;
  }
  bool exist = false;
  while(idx<values.size()) {
    int i = idx;
    ++idx;
    if(values[i].size() == 0) {
      continue;
    }
    char chunk[SEGMENT_MAX_SIZE];
    Segment* segment = reinterpret_cast<Segment*>(chunk);
    memcpy(chunk, values[i].c_str(), values[i].size());
    switch(key_order) {
      case P:
        {
          if(subject != nullptr) {
            x_block_no = segment->x_first_block_no;
          }
          if(object != nullptr) {
            y_block_no = segment->y_first_block_no;
          }

          if(segment->dsize != 0) {
            // Prefetch next nodes
            if(x_block_no != 0) {
              this->table.data_file.prefetchNode(x_block_no);
            }
            if(y_block_no != 0) {
              this->table.data_file.prefetchNode(y_block_no);
            }

            int x = 0;
            int y = sizeof(uint32_t);
            while(y < segment->dsize) {
              const uint32_t *entry_x = reinterpret_cast<const uint32_t*>(segment->data + x);
              const uint32_t *entry_y = reinterpret_cast<const uint32_t*>(segment->data + y);
              if(subject != nullptr) {
                subject->column.push_back(*entry_x);
              }
              if(object != nullptr) {
                object->column.push_back(*entry_y);
              }
              x += sizeof(uint32_t);
              y += sizeof(uint32_t);
            }
            return true;
          }
        }
        break;
      case SP:
        {
          subject->id = key_ids[i];
          if(object != nullptr) {
            y_block_no = segment->y_first_block_no;

            if(segment->dsize != 0) {
              // Prefetch next nodes
              if(y_block_no != 0) {
                this->table.data_file.prefetchNode(y_block_no);
              }

              object->column.insert(object->column.end(), reinterpret_cast<uint32_t*>(segment->data), reinterpret_cast<uint32_t*>(segment->data)+segment->dsize/sizeof(uint32_t));

              for(int k=subject->column.size(); k<object->column.size(); k++) {
                subject->column.push_back(subject->id);
              }
              return true;
            }
          } else {
            exist = true;
            /*
            for(int k = 0; k<segment->count; k++) {
              subject->column.push_back(subject->id);
            }
            */
            subject->column.push_back(subject->id);
            continue;
          }
        }
        break;
      case OP:
        {
          object->id = key_ids[i];
          if(subject != nullptr) {
            x_block_no = segment->x_first_block_no;

            if(segment->dsize != 0) {
              // Prefetch next nodes
              if(x_block_no != 0) {
                this->table.data_file.prefetchNode(x_block_no);
              }

              subject->column.insert(subject->column.end(), reinterpret_cast<uint32_t*>(segment->data), reinterpret_cast<uint32_t*>(segment->data)+segment->dsize/sizeof(uint32_t));

              for(int k=object->column.size(); k<subject->column.size(); k++) {
                object->column.push_back(object->id);
              }
              return true;
            }
          } else {
            exist = true;
            /*
            for(int k = 0; k<segment->count; k++) {
              object->column.push_back(object->id);
            }
            */
            object->column.push_back(object->id);
            continue;
          }
        }
        break;
    }

    if(read()) {
      return true;
    }
    return false;
  }
  return exist;
}

bool TripleTable::read(TripleOrder key_order, Resource *subject, Resource *predicate, Resource *object) {
  bool exist = false;
  switch(key_order) {
    case P:
      {
        Key key = { 1, predicate->id, 0};
        std::string value;
        char chunk[SEGMENT_MAX_SIZE];
        Segment* segment = reinterpret_cast<Segment*>(chunk);
        if(!this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          return false;
        }
        memcpy(chunk, value.c_str(), value.size());
        if(segment->dsize != 0) {
          exist = true;

          int x = 0;
          int y = sizeof(uint32_t);
          while(y < segment->dsize) {
            const uint32_t *entry_x = reinterpret_cast<const uint32_t*>(segment->data + x);
            const uint32_t *entry_y = reinterpret_cast<const uint32_t*>(segment->data + y);
            if(subject != nullptr) {
              subject->column.push_back(*entry_x);
            }
            if(object == nullptr) {
              object->column.push_back(*entry_y);
            }
            x += sizeof(uint32_t);
            y += sizeof(uint32_t);
          }
        }
        if(subject != nullptr) {
          if(readData(segment, 1, subject->column)) {
            exist = true;
          }
        }
        if(object != nullptr) {
          if(readData(segment, 2, object->column)) {
            exist = true;
          }
        }
        return exist;
      }
      break;
    case SP:
      {
        if(subject->column.size() > 0) {
          std::vector<uint32_t> subject_ids = subject->column;
          std::vector<std::string> keys(subject_ids.size());
          Key key = { 2, 0, predicate->id};
          for(int i=0; i<subject_ids.size(); i++) {
            key.x = subject_ids[i];
            keys[i] = std::string(reinterpret_cast<char*>(&key), KEY_SIZE);
          }
          std::vector<std::string> values;
          if(!this->kvstore.multiGet(keys, &values)){
            return false;
          }

          subject->column.clear();
          char chunk[SEGMENT_MAX_SIZE];
          Segment* segment = reinterpret_cast<Segment*>(chunk);
          if(object != nullptr) {
            int k = 0;
            for(int i=0; i<subject_ids.size(); i++) {
              if(values[i].size() == 0) {
                continue;
              }
              memcpy(chunk, values[i].c_str(), values[i].size());
              if(segment->dsize != 0) {
                exist = true;

                int pos = 0;
                while(pos < segment->dsize) {
                  const uint32_t *entry = reinterpret_cast<const uint32_t*>(segment->data + pos);
                  object->column.push_back(*entry);
                  pos += sizeof(uint32_t);
                }
              }
              if(readData(segment, 2, object->column)) {
                exist = true;
              }
              for(; k<object->column.size(); k++) {
                subject->column.push_back(subject_ids[i]);
              }
            }
          } else {
            for(int i=0; i<subject_ids.size(); i++) {
              if(values[i].size() == 0) {
                continue;
              }
              memcpy(chunk, values[i].c_str(), values[i].size());
              exist = true;
              for(int k = 0; k<segment->count; k++) {
                subject->column.push_back(subject_ids[i]);
              }
            }
          }
          return exist;
        } else {
          Key key = { 2, subject->id, predicate->id};
          std::string value;
          char chunk[SEGMENT_MAX_SIZE];
          Segment* segment = reinterpret_cast<Segment*>(chunk);
          if(!this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
            return false;
          }
          memcpy(chunk, value.c_str(), value.size());

          if(object != nullptr) {
            if(segment->dsize != 0) {
              exist = true;

              int pos = 0;
              while(pos < segment->dsize) {
                const uint32_t *entry = reinterpret_cast<const uint32_t*>(segment->data + pos);
                object->column.push_back(*entry);
                pos += sizeof(uint32_t);
              }
            }
            if(readData(segment, 2, object->column)) {
              exist = true;
            }
            for(int j=0; j<object->column.size(); j++) {
              subject->column.push_back(subject->id);
            }
          } else {
            for(int j = 0; j<segment->count; j++) {
              subject->column.push_back(subject->id);
            }
          }
        }
        return exist;
      }
      break;
    case OP:
      {
        if(object->column.size() > 0) {
          std::vector<uint32_t> object_ids = object->column;
          std::vector<std::string> keys(object_ids.size());
          Key key = { 4, 0, predicate->id};
          for(int i=0; i<object_ids.size(); i++) {
            key.x = object_ids[i];
            keys[i] = std::string(reinterpret_cast<char*>(&key), KEY_SIZE);
          }
          std::vector<std::string> values;
          if(!this->kvstore.multiGet(keys, &values)){
            return false;
          }

          object->column.clear();
          char chunk[SEGMENT_MAX_SIZE];
          Segment* segment = reinterpret_cast<Segment*>(chunk);
          if(subject != nullptr) {
            int k = 0;
            for(int i=0; i<object_ids.size(); i++) {
              if(values[i].size() == 0) {
                continue;
              }
              memcpy(chunk, values[i].c_str(), values[i].size());

              if(segment->dsize != 0) {
                exist = true;

                int pos = 0;
                while(pos < segment->dsize) {
                  const uint32_t *entry = reinterpret_cast<const uint32_t*>(segment->data + pos);
                  subject->column.push_back(*entry);
                  pos += sizeof(uint32_t);
                }
              }

              if(readData(segment, 1, subject->column)) {
                exist = true;
              }

              for(; k<subject->column.size(); k++) {
                object->column.push_back(object_ids[i]);
              }
            }
          } else {
            for(int i=0; i<object_ids.size(); i++) {
              if(values[i].size() == 0) {
                continue;
              }
              memcpy(chunk, values[i].c_str(), values[i].size());
              exist = true;

              for(int k=0; k<segment->count; k++) {
                object->column.push_back(object_ids[i]);
              }
            }
          }
          return exist;
        } else {
          Key key = { 4, object->id, predicate->id};
          std::string value;
          char chunk[SEGMENT_MAX_SIZE];
          Segment* segment = reinterpret_cast<Segment*>(chunk);
          if(!this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
            return false;
          }
          memcpy(chunk, value.c_str(), value.size());

          if(subject != nullptr) {
            if(segment->dsize != 0) {
              exist = true;

              int pos = 0;
              while(pos < segment->dsize) {
                const uint32_t *entry = reinterpret_cast<const uint32_t*>(segment->data + pos);
                subject->column.push_back(*entry);
                pos += sizeof(uint32_t);
              }
            }
            if(readData(segment, 1, subject->column)) {
              exist = true;
            }
            for(int j=0; j<subject->column.size(); j++) {
              object->column.push_back(object->id);
            }
          } else {
            for(int j=0; j<segment->count; j++) {
              object->column.push_back(object->id);
            }
          }
        }
        return exist;
      }
      break;
  }
  return exist;
}

void TripleTable::write(const Resource& subject, const Resource& predicate, const Resource& object) {
}

void TripleTable::write(TripleOrder key_order, const Resource& subject, const Resource& predicate, const Resource& object) {
  switch(key_order) {
    case P:
      {
        Key key = { 1, predicate.id, 0};
        std::string value;
        char chunk[SEGMENT_MAX_SIZE];
        Segment* segment = reinterpret_cast<Segment*>(chunk);
        if(this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          memcpy(chunk, value.c_str(), value.size());
        } else {
          segment->count = 0;
          segment->x_distinct_count = 0;
          segment->y_distinct_count = 0;
          segment->x_first_block_no = 0;
          segment->x_last_block_no = 0;
          segment->y_first_block_no = 0;
          segment->y_last_block_no = 0;
          segment->x_index_block_no = 0;
          segment->y_index_block_no = 0;
          segment->dtype = 0;
          segment->dsize = 0;
        }
        writeData(segment, 1, subject.column);
        writeData(segment, 2, object.column);
        segment->count += subject.column.size();

        RoaringBitVector sub_bitvec;
        readBitVector(segment, ResourcePosition::SUBJECT, sub_bitvec);
        sub_bitvec.add(subject.column);
        writeBitVector(segment, ResourcePosition::SUBJECT, sub_bitvec);

        RoaringBitVector obj_bitvec;
        readBitVector(segment, ResourcePosition::OBJECT, obj_bitvec);
        obj_bitvec.add(object.column);
        writeBitVector(segment, ResourcePosition::OBJECT, obj_bitvec);

        this->kvstore.put(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), std::string(chunk, SEGMENT_HEADER_SIZE + segment->dsize));
      }
      break;
    case SP:
      {
        Key key = { 2, subject.id, predicate.id};
        std::string value;
        char chunk[SEGMENT_MAX_SIZE];
        Segment* segment = reinterpret_cast<Segment*>(chunk);
        if(this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          memcpy(chunk, value.c_str(), value.size());
        } else {
          segment->count = 0;
          segment->x_distinct_count = 0;
          segment->y_distinct_count = 0;
          segment->x_first_block_no = 0;
          segment->x_last_block_no = 0;
          segment->y_first_block_no = 0;
          segment->y_last_block_no = 0;
          segment->x_index_block_no = 0;
          segment->y_index_block_no = 0;
          segment->dtype = 0;
          segment->dsize = 0;
        }

        writeData(segment, 2, object.column);
        segment->count += object.column.size();
        this->kvstore.put(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), std::string(chunk, SEGMENT_HEADER_SIZE + segment->dsize));
      }
      break;
    case OP:
      {
        Key key = { 4, object.id, predicate.id};
        std::string value;
        char chunk[SEGMENT_MAX_SIZE];
        Segment* segment = reinterpret_cast<Segment*>(chunk);
        if(this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          memcpy(chunk, value.c_str(), value.size());
        } else {
          segment->count = 0;
          segment->x_distinct_count = 0;
          segment->y_distinct_count = 0;
          segment->x_first_block_no = 0;
          segment->x_last_block_no = 0;
          segment->y_first_block_no = 0;
          segment->y_last_block_no = 0;
          segment->x_index_block_no = 0;
          segment->y_index_block_no = 0;
          segment->dtype = 0;
          segment->dsize = 0;
        }

        writeData(segment, 1, subject.column);
        segment->count += subject.column.size();
        this->kvstore.put(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), std::string(chunk, SEGMENT_HEADER_SIZE + segment->dsize));
      }
      break;
  }
}

int TripleTable::count(TripleOrder key_order, uint32_t subject, uint32_t predicate, uint32_t object) {
  switch(key_order) {
    case P:
      {
        Key key = { 1, predicate, 0};
        std::string value;
        char chunk[SEGMENT_MAX_SIZE];
        Segment* segment = reinterpret_cast<Segment*>(chunk);
        if(!this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          return 0;
        }
        memcpy(chunk, value.c_str(), value.size());
        return segment->count;
      }
      break;
    case SP:
      {
        Key key = { 2, subject, predicate};
        std::string value;
        char chunk[SEGMENT_MAX_SIZE];
        Segment* segment = reinterpret_cast<Segment*>(chunk);
        if(!this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          return 0;
        }
        memcpy(chunk, value.c_str(), value.size());
        return segment->count;
      }
      break;
    case OP:
      {
        Key key = { 4, object, predicate};
        std::string value;
        char chunk[SEGMENT_MAX_SIZE];
        Segment* segment = reinterpret_cast<Segment*>(chunk);
        if(!this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          return 0;
        }
        memcpy(chunk, value.c_str(), value.size());
        return segment->count;
      }
      break;
  }
  return 0;
}

int TripleTable::distinctCount(TripleOrder key_order, uint32_t subject, uint32_t predicate, uint32_t object) {
  switch(key_order) {
    case SP:
      {
        Key key = { 1, predicate, 0};
        std::string value;
        char chunk[SEGMENT_MAX_SIZE];
        Segment* segment = reinterpret_cast<Segment*>(chunk);
        if(!this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          return 0;
        }
        memcpy(chunk, value.c_str(), value.size());
        return segment->x_distinct_count;
      }
      break;
    case OP:
      {
        Key key = { 1, predicate, 0};
        std::string value;
        char chunk[SEGMENT_MAX_SIZE];
        Segment* segment = reinterpret_cast<Segment*>(chunk);
        if(!this->kvstore.get(std::string(reinterpret_cast<char*>(&key), KEY_SIZE), &value)){
          return 0;
        }
        memcpy(chunk, value.c_str(), value.size());
        return segment->y_distinct_count;
      }
      break;
  }
  return 0;
}

const uint32_t TripleTable::KEY_SIZE = sizeof(uint32_t) * 2 + sizeof(uint8_t);

const uint32_t TripleTable::SEGMENT_MAX_SIZE = 4096;
const uint32_t TripleTable::SEGMENT_HEADER_SIZE = sizeof(uint32_t) * 9 + sizeof(uint16_t) * 2;
const uint32_t TripleTable::SEGMENT_DATA_MAX_SIZE = SEGMENT_MAX_SIZE - TripleTable::SEGMENT_HEADER_SIZE;

const uint32_t TripleTable::NODE_HEADER_SIZE = sizeof(uint32_t) * 2 + sizeof(uint16_t) * 2;
const uint32_t TripleTable::NODE_DATA_MAX_SIZE = BLOCK_SIZE - TripleTable::NODE_HEADER_SIZE;


bool TripleTable::readNode(uint32_t block_no, Node* node) {
  return true;
}
bool TripleTable::writeNode(uint32_t block_no, Node* node) {
  return true;
}

bool TripleTable::readData(const Segment* segment, int pos, std::vector<uint32_t>& column) {
  uint32_t first_block_no = 0;
  if(pos == 1) {
    first_block_no = segment->x_first_block_no;
  } else if(pos == 2) {
    first_block_no = segment->y_first_block_no;
  }
  bool exist = false;
  if(first_block_no != 0) {
    exist = true;
    BufferPage* page = this->data_file.getNode(first_block_no);
    char* block = page->getBlockData();
    Node* node = reinterpret_cast<Node*>(block);
    int pos = 0;
    while(true) {
      if(node->next_block_no != 0) {
        this->data_file.prefetchNode(node->next_block_no);
      }

      column.insert(column.end(), reinterpret_cast<uint32_t*>(node->data), reinterpret_cast<uint32_t*>(node->data)+node->dsize/sizeof(uint32_t));

      if(node->next_block_no == 0) {
        break;
      }
      this->data_file.updateNode(page, false, true);
      page = this->data_file.getNode(node->next_block_no);
      block = page->getBlockData();
      node = reinterpret_cast<Node*>(block);
    }
  }
  return exist;
}

void TripleTable::writeData(Segment* segment, int pos, const std::vector<uint32_t>& column) {
  uint32_t last_block_no = 0;
  if(pos == 1) {
    last_block_no = segment->x_last_block_no;
  } else if(pos == 2) {
    last_block_no = segment->y_last_block_no;
  }

  uint32_t entry;
  if(segment->dsize + column.size() * sizeof(uint32_t) < SEGMENT_DATA_MAX_SIZE && last_block_no == 0) {
    for(int i=0; i<column.size(); i++) {
      entry = column[i];

      memcpy(segment->data + segment->dsize, reinterpret_cast<char*>(&entry), sizeof(uint32_t));
      segment->dsize += sizeof(uint32_t);
    }
    return;
  }

  BufferPage* page;
  char* block;
  Node* node;
  if(last_block_no != 0) {
    page = this->data_file.getNode(last_block_no);
    block = page->getBlockData();
    node = reinterpret_cast<Node*>(block);
  } else {
    page = this->data_file.appendNode();
    if(pos == 1) {
      segment->x_first_block_no = page->getBlockNo();
    }else if(pos == 2) {
      segment->y_first_block_no = page->getBlockNo();
    }
    block = page->getBlockData();
    node = reinterpret_cast<Node*>(block);
    node->block_no = page->getBlockNo();
    node->next_block_no = 0;
    node->dtype = 0;
    node->dsize = 0;
  }

  for(int i=0; i<column.size(); i++) {
    entry = column[i];

    if(node->dsize + sizeof(uint32_t) >= NODE_DATA_MAX_SIZE) {
      BufferPage* new_page = this->data_file.appendNode();
      node->next_block_no = new_page->getBlockNo();
      this->data_file.updateNode(page, true, true);

      page = new_page;
      block = page->getBlockData();
      node = reinterpret_cast<Node*>(block);
      node->block_no = page->getBlockNo();
      node->next_block_no = 0;
      node->dtype = 0;
      node->dsize = 0;
    }

    memcpy(node->data + node->dsize, reinterpret_cast<char*>(&entry), sizeof(uint32_t));
    node->dsize += sizeof(uint32_t);
  }

  if(pos == 1) {
    segment->x_last_block_no = page->getBlockNo();
  } else if(pos == 2) {
    segment->y_last_block_no = page->getBlockNo();
  }

  this->data_file.updateNode(page, true, true);
}

bool TripleTable::readBitVector(Segment* segment, ResourcePosition pos, RoaringBitVector& bitvec) {
  switch(pos) {
    case SUBJECT:
      if(segment->x_index_block_no != 0) {
        BufferPage* page = this->index_file.getNode(segment->x_index_block_no);
        char* block = page->getBlockData();
        Node* node = reinterpret_cast<Node*>(block);

        ByteBuffer buf;
        while(true) {
          buf.append(node->data, node->dsize);

          if(node->next_block_no == 0) {
            break;
          }
          this->index_file.updateNode(page, false, true);
          page = this->index_file.getNode(node->next_block_no);
          block = page->getBlockData();
          node = reinterpret_cast<Node*>(block);
        }

        RoaringBitVector::deserialize(bitvec, buf);
        return true;
      }
      break;
    case OBJECT:
      if(segment->y_index_block_no != 0) {
        BufferPage* page = this->index_file.getNode(segment->y_index_block_no);
        char* block = page->getBlockData();
        Node* node = reinterpret_cast<Node*>(block);

        ByteBuffer buf;
        while(true) {
          buf.append(node->data, node->dsize);

          if(node->next_block_no == 0) {
            break;
          }
          this->index_file.updateNode(page, false, true);
          page = this->index_file.getNode(node->next_block_no);
          block = page->getBlockData();
          node = reinterpret_cast<Node*>(block);
        }

        RoaringBitVector::deserialize(bitvec, buf);
        return true;
      }
      break;
  }
  return false;
}

void TripleTable::writeBitVector(Segment* segment, ResourcePosition pos, const RoaringBitVector& bitvec) {
  ByteBuffer buf;
  RoaringBitVector::serialize(bitvec, buf);

  char* buf_ptr = buf.data();
  int buf_size = buf.size();

  if(buf_size == 0) {
    return;
  }

  BufferPage* page;
  char* block;
  Node* node;

  switch(pos) {
    case SUBJECT:
      {
        segment->x_distinct_count = (uint32_t)bitvec.cardinality();
        if(segment->x_index_block_no != 0) {
          page = this->index_file.getNode(segment->x_index_block_no);
          block = page->getBlockData();
          node = reinterpret_cast<Node*>(block);
        } else {
          page = this->index_file.appendNode();
          segment->x_index_block_no = page->getBlockNo();
          block = page->getBlockData();
          node = reinterpret_cast<Node*>(block);
          node->block_no = page->getBlockNo();
          node->next_block_no = 0;
          node->dtype = 0;
          node->dsize = 0;
        }
      }
      break;
    case OBJECT:
      {
        segment->y_distinct_count = (uint32_t)bitvec.cardinality();
        if(segment->y_index_block_no != 0) {
          page = this->index_file.getNode(segment->y_index_block_no);
          block = page->getBlockData();
          node = reinterpret_cast<Node*>(block);
        } else {
          page = this->index_file.appendNode();
          segment->y_index_block_no = page->getBlockNo();
          block = page->getBlockData();
          node = reinterpret_cast<Node*>(block);
          node->block_no = page->getBlockNo();
          node->next_block_no = 0;
          node->dtype = 0;
          node->dsize = 0;
        }
      }
      break;
  }

  while(true) {
    if(buf_size > NODE_DATA_MAX_SIZE) {
      node->dsize = NODE_DATA_MAX_SIZE;
      memcpy(node->data, buf_ptr, NODE_DATA_MAX_SIZE);
      buf_ptr += NODE_DATA_MAX_SIZE;
      buf_size -= NODE_DATA_MAX_SIZE;
    } else {
      node->dsize = buf_size;
      memcpy(node->data, buf_ptr, buf_size);
      this->index_file.updateNode(page, true, true);
      break;
    }

    if(node->next_block_no != 0) {
      this->index_file.updateNode(page, true, true);
      page = this->index_file.getNode(node->next_block_no);
      block = page->getBlockData();
      node = reinterpret_cast<Node*>(block);
    } else {
      BufferPage* new_page = this->index_file.appendNode();
      node->next_block_no = new_page->getBlockNo();
      this->index_file.updateNode(page, true, true);

      page = new_page;
      block = page->getBlockData();
      node = reinterpret_cast<Node*>(block);
      node->block_no = page->getBlockNo();
      node->next_block_no = 0;
      node->dtype = 0;
      node->dsize = 0;
    }
  }
}




// Heap File
TripleTable::HeapFile::HeapFile(const std::string& file_path, BufferManager* buffer_manager)
  : file_path(file_path), file(file_path), buffer_manager(buffer_manager) {}

TripleTable::HeapFile::~HeapFile() {}

bool TripleTable::HeapFile::open() {
  if(!File::exist(file_path)) {
    if(!create(FILE_GROWTH, 0)) {
      return false;
    }
  }
  // file.open();
  if(!file.open(O_RDWR)) {
    return false;
  }

  header = reinterpret_cast<Header*>(new char[HEADER_SIZE]);
  if(!readHeader()) {
    return false;
  }
  return true;
}

bool TripleTable::HeapFile::close() {
  writeHeader();
  delete[] reinterpret_cast<char*>(header);
  this->buffer_manager->flushFile(&file, true);
  file.close();
  return true;
}

BufferPage* TripleTable::HeapFile::appendNode() {
  BufferPage* page = nullptr;
  mutex.lock();
  uint32_t block_no = header->num_block;
  header->num_block++;
  uint64_t fsize = header->num_block * BLOCK_SIZE;
  if(fsize > header->file_size) {
    fsize = (header->num_block + FILE_GROWTH) * BLOCK_SIZE;
    file.truncate(fsize);
    header->file_size = fsize;
  }
  page = buffer_manager->allocBufferPage(&this->file, block_no, false);
  mutex.unlock();
  return page;
}

BufferPage* TripleTable::HeapFile::getNode(uint32_t block_no) {
  return this->buffer_manager->getBufferPage(&this->file, block_no, true);
}

void TripleTable::HeapFile::prefetchNode(uint32_t block_no) {
  this->file.prefetch(block_no * BLOCK_SIZE, BLOCK_SIZE);
}

void TripleTable::HeapFile::updateNode(BufferPage* page, bool dirty, bool exclusive) {
  this->buffer_manager->unfixBufferPage(page, dirty, exclusive);
}

const uint32_t TripleTable::HeapFile::HEADER_SIZE = sizeof(uint32_t) * 4 + sizeof(uint64_t);
const uint32_t TripleTable::HeapFile::FILE_GROWTH = 20;

bool TripleTable::HeapFile::create(uint32_t capacity, uint32_t version) {
  if(capacity < 1) {
    capacity = FILE_GROWTH;
  }
  file.open(O_WRONLY|O_CREAT);
  header = reinterpret_cast<Header*>(new char[HEADER_SIZE]);
  header->version = version;
  header->capacity = capacity;
  header->block_size = BLOCK_SIZE;
  header->num_block = 1;
  header->file_size = (header->capacity) * BLOCK_SIZE;
  file.truncate(header->file_size);
  writeHeader();
  delete[] reinterpret_cast<char*>(header);
  file.close();
  return true;
}

bool TripleTable::HeapFile::readHeader() {
  this->file.read(reinterpret_cast<char*>(header), HEADER_SIZE, 0);
  return true;
}

bool TripleTable::HeapFile::writeHeader() {
  this->file.write(reinterpret_cast<const char*>(header), HEADER_SIZE, 0);
  return true;
}
