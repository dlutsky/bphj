#ifndef BITMAP_INDEX_H
#define BITMAP_INDEX_H


#include <string>
#include <cstdint>
#include <sys/types.h>
#include <memory>
#include <map>
#include <utility>
#include "common/constants.h"
#include "bitmap/roaring_bitvector.h"
#include "util/file_directory.h"
#include "thread/mutex.h"
#include "buffer/buffer_manager.h"



class BitMapIndex {
public:
  BitMapIndex(const std::string& store_path, const std::string& file_name, uint32_t version, BufferManager* buffer_manager);
  ~BitMapIndex();

  bool open();
  bool close();

  RoaringBitVector get(uint32_t idx);
  bool set(uint32_t idx, RoaringBitVector& bitvec);
  uint32_t append(RoaringBitVector& bitvec);

private:
  #pragma pack(push, 1)
  struct Header {
    uint32_t version;
    uint32_t compr_format;
    uint32_t capacity;
    uint32_t block_size;
    uint32_t num_block;
    uint32_t inner_root;
    uint32_t inner_level;
    uint32_t num_bitvec;
    uint64_t file_size;
  };
  struct InnerNode {
    uint32_t indexes[1];
  };
  struct IndexNode {
    uint32_t block_no;
    uint32_t next_block_no;
    uint32_t dsize;
    char* data;
  };
  #pragma pack(pop)

  static const uint32_t HEADER_SIZE;
  static const uint32_t FILE_GROWTH;
  static const uint32_t INNER_NODE_SHIFT;
  static const uint32_t INNER_NODE_MASK;
  static const uint32_t INDEX_NODE_HEADER_SIZE;
  static const uint32_t DATA_MAX_SIZE;

  bool create(uint32_t capacity, uint32_t version);

  bool readHeader();
  RoaringBitVector readBitVector(uint32_t block_no);

  bool writeHeader();
  bool writeBitVector(uint32_t block_no, const RoaringBitVector& bitvec);
  uint32_t appendBitVector(const RoaringBitVector& bitvec);

  BufferPage* getInnerNodePage(uint32_t idx);
  BufferPage* createPage();

  std::string file_path;
  uint32_t version;
  RandomRWFile file;

  Header* header;
  BufferManager* buffer_manager;

  Mutex mutex;
};


#endif
