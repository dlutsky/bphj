#include "bitmap_index.h"
#include "util/byte_buffer.h"


BitMapIndex::BitMapIndex(const std::string& store_path, const std::string& file_name, uint32_t version, BufferManager* buffer_manager)
  : file_path(store_path + "/" + file_name + "_" + std::to_string(version) + ".gbmp"), version(version), file(file_path), buffer_manager(buffer_manager) {}

BitMapIndex::~BitMapIndex() {}


bool BitMapIndex::open() {
  if(!File::exist(file_path)) {
    if(!create(FILE_GROWTH, version)) {
      return false;
    }
  }
  // file.open();
  file.open(O_RDWR);
  header = reinterpret_cast<Header*>(new char[HEADER_SIZE]);
  readHeader();
  return true;
}

bool BitMapIndex::close() {
  writeHeader();
  delete[] reinterpret_cast<char*>(header);
  buffer_manager->flushFile(&file, true);
  file.close();
  return true;
}

RoaringBitVector BitMapIndex::get(uint32_t idx) {
  BufferPage* page = getInnerNodePage(idx);
  if(page == nullptr) {
    return RoaringBitVector();
  }
  InnerNode* inner_node = reinterpret_cast<InnerNode*>(page->getBlockData());
  idx &= INNER_NODE_MASK;
  uint32_t index_block_no = inner_node->indexes[idx];
  this->buffer_manager->unfixBufferPage(page, false, true);
  return readBitVector(index_block_no);
}

bool BitMapIndex::set(uint32_t idx, RoaringBitVector& bitvec) {
  BufferPage* page = getInnerNodePage(idx);
  InnerNode* inner_node = reinterpret_cast<InnerNode*>(page->getBlockData());
  idx &= INNER_NODE_MASK;
  uint32_t index_block_no = inner_node->indexes[idx];
  this->buffer_manager->unfixBufferPage(page, false, true);
  if(!index_block_no) {
    return false;
  }
  writeBitVector(index_block_no, bitvec);
  return true;
}

uint32_t BitMapIndex::append(RoaringBitVector& bitvec) {
  uint32_t idx = 0;
  mutex.lock();
  idx = header->num_bitvec;
  header->num_bitvec++;
  mutex.unlock();

  BufferPage* page = getInnerNodePage(idx);
  InnerNode* inner_node = reinterpret_cast<InnerNode*>(page->getBlockData());
  idx &= INNER_NODE_MASK;
  uint32_t block_no = appendBitVector(bitvec);
  inner_node->indexes[idx] = block_no;
  this->buffer_manager->unfixBufferPage(page, true, true);
  return block_no;
}


const uint32_t BitMapIndex::HEADER_SIZE = sizeof(uint32_t) * 8 + sizeof(uint64_t);
const uint32_t BitMapIndex::FILE_GROWTH = 20;
const uint32_t BitMapIndex::INNER_NODE_SHIFT = 10;
const uint32_t BitMapIndex::INNER_NODE_MASK = 1023;
const uint32_t BitMapIndex::INDEX_NODE_HEADER_SIZE = sizeof(uint32_t) * 3;
const uint32_t BitMapIndex::DATA_MAX_SIZE = BLOCK_SIZE - BitMapIndex::INDEX_NODE_HEADER_SIZE;


bool BitMapIndex::create(uint32_t capacity, uint32_t version) {
  if(capacity < 2) {
    capacity = FILE_GROWTH;
  }
  file.open(O_WRONLY|O_CREAT);
  header = reinterpret_cast<Header*>(new char[HEADER_SIZE]);
  header->version = version;
  header->compr_format = 0;
  header->capacity = capacity;
  header->block_size = BLOCK_SIZE;
  // header->num_block = capacity >> 7 + (capacity & 127 != 0) + 1;     // each inner block contains 128 indexes
  header->num_block = 2;
  header->inner_root = 1;
  header->inner_level = 1;
  header->file_size = (header->capacity) * BLOCK_SIZE;
  file.truncate(header->file_size);
  writeHeader();
  delete[] reinterpret_cast<char*>(header);
  file.close();
  return true;
}

bool BitMapIndex::readHeader() {
  file.read(reinterpret_cast<char*>(header), HEADER_SIZE, 0);
  return true;
}

RoaringBitVector BitMapIndex::readBitVector(uint32_t block_no) {
  if(block_no == 0 || block_no > header->num_block) {
    return RoaringBitVector();
  }

  BufferPage* page = this->buffer_manager->getBufferPage(&file, block_no, true);
  IndexNode* idx_node = reinterpret_cast<IndexNode*>(page->getBlockData());

  ByteBuffer buf;
  buf.reserve(idx_node->dsize);
  while(true) {
    if(idx_node->next_block_no != 0) {
      buf.append(idx_node->data, DATA_MAX_SIZE);
      this->buffer_manager->unfixBufferPage(page, false, true);

      page = this->buffer_manager->getBufferPage(&file, idx_node->next_block_no, true);
      idx_node = reinterpret_cast<IndexNode*>(page->getBlockData());
    }
    else {
      buf.append(idx_node->data, idx_node->dsize);
      this->buffer_manager->unfixBufferPage(page, false, true);
      break;
    }
  }
  buf.resize(idx_node->dsize);

  return RoaringBitVector::deserialize(buf);
}



bool BitMapIndex::writeHeader() {
  file.write(reinterpret_cast<const char*>(header), HEADER_SIZE, 0);
  return true;
}

bool BitMapIndex::writeBitVector(uint32_t block_no, const RoaringBitVector& bitvec) {
  ByteBuffer buf;
  RoaringBitVector::serialize(bitvec, buf);

  char* buf_ptr = buf.data();
  int buf_size = buf.size();

  if(!buf_size) {
    return false;
  }

  BufferPage* page = this->buffer_manager->getBufferPage(&file, block_no, true);
  IndexNode* idx_node = reinterpret_cast<IndexNode*>(page->getBlockData());
  idx_node->dsize = buf_size;
  while(true) {
    if(buf_size > DATA_MAX_SIZE) {
      memcpy(idx_node->data, buf_ptr, DATA_MAX_SIZE);
      buf_ptr += DATA_MAX_SIZE;
      buf_size -= DATA_MAX_SIZE;
      if(idx_node->next_block_no != 0) {
        this->buffer_manager->unfixBufferPage(page, true, true);
        page = this->buffer_manager->getBufferPage(&file, idx_node->next_block_no, true);
        idx_node = reinterpret_cast<IndexNode*>(page->getBlockData());
      }
      else {
        BufferPage* new_page = createPage();
        idx_node->next_block_no = new_page->getBlockNo();
        this->buffer_manager->unfixBufferPage(page, true, true);
        page = new_page;
        idx_node = reinterpret_cast<IndexNode*>(page->getBlockData());
        idx_node->block_no = new_page->getBlockNo();
      }
    }
    else {
      idx_node->dsize = buf_size;
      memcpy(idx_node->data, buf_ptr, buf_size);
      this->buffer_manager->unfixBufferPage(page, true, true);
      break;
    }
  }

  return true;
}

uint32_t BitMapIndex::appendBitVector(const RoaringBitVector& bitvec) {
  ByteBuffer buf;
  RoaringBitVector::serialize(bitvec, buf);

  char* buf_ptr = buf.data();
  int buf_size = buf.size();

  if(!buf_size) {
    return 0;
  }

  uint32_t first_block_no = 0;

  BufferPage *prev_page = nullptr, *page = createPage();
  IndexNode *prev_node = nullptr, *idx_node = reinterpret_cast<IndexNode*>(page->getBlockData());
  idx_node->block_no = page->getBlockNo();
  idx_node->dsize = buf_size;

  first_block_no = page->getBlockNo();

  while(true) {
    if(buf_size > DATA_MAX_SIZE) {
      memcpy(idx_node->data, buf_ptr, DATA_MAX_SIZE);
      buf_ptr += DATA_MAX_SIZE;
      buf_size -= DATA_MAX_SIZE;
      if(prev_page != nullptr) {
        prev_node->next_block_no = page->getBlockNo();
        this->buffer_manager->unfixBufferPage(prev_page, true, true);
      }
      prev_page = page;
      prev_node = idx_node;
      page = createPage();
      idx_node = reinterpret_cast<IndexNode*>(page->getBlockData());
    }
    else {
      idx_node->dsize = buf_size;
      memcpy(idx_node->data, buf_ptr, buf_size);
      this->buffer_manager->unfixBufferPage(page, true, true);
      break;
    }
  }

  return first_block_no;
}

BufferPage* BitMapIndex::getInnerNodePage(uint32_t idx) {
  mutex.lock();
  uint32_t i = idx >> (INNER_NODE_SHIFT*this->header->inner_level);
  if(i != 0) {
    i &= INNER_NODE_MASK;
    BufferPage* page = createPage();
    InnerNode* inner_node = reinterpret_cast<InnerNode*>(page->getBlockData());
    page = createPage();
    inner_node->indexes[i] = page->getBlockNo();
  }
  uint32_t inner_block_no = this->header->inner_root;
  uint32_t level = this->header->inner_level - 1;
  mutex.unlock();

  BufferPage* page = this->buffer_manager->getBufferPage(&file, inner_block_no, true);
  InnerNode* inner_node = reinterpret_cast<InnerNode*>(page->getBlockData());
  while(level != 0) {
    i = idx >> (INNER_NODE_SHIFT*level);
    i &= INNER_NODE_MASK;
    inner_block_no = inner_node->indexes[i];
    this->buffer_manager->unfixBufferPage(page, false, true);
    page = this->buffer_manager->getBufferPage(&file, inner_block_no, true);
    inner_node = reinterpret_cast<InnerNode*>(page->getBlockData());
    level--;
  }

  return page;
}

BufferPage* BitMapIndex::createPage() {
  mutex.lock();
  uint32_t block_no = header->num_block;
  header->num_block++;
  uint64_t fsize = header->num_block * BLOCK_SIZE;
  if(fsize > header->file_size) {
    fsize = (header->num_block + 10) * BLOCK_SIZE;
    file.truncate(fsize);
    header->file_size = fsize;
  }
  BufferPage* page = this->buffer_manager->allocBufferPage(&file, block_no, false);
  mutex.unlock();
  return page;
}
