#ifndef TRIPLE_TABLE_H
#define TRIPLE_TABLE_H

#include <string>
#include "common/triple.h"
#include "common/resource.h"
#include "common/constants.h"
#include "buffer/buffer_manager.h"
#include "kvstore/rocksdb_store.h"
#include "bitmap/roaring_bitvector.h"
#include "util/bdb_file.h"
#include "util/file_directory.h"
#include "thread/mutex.h"



class TripleTable {
public:
  TripleTable(const std::string& store_path, const std::string& table_name, BufferManager* buffer_manager);
  ~TripleTable();

  bool open();
  bool close();

  class BlockScanner {
  public:
    BlockScanner(TripleTable& table, TripleOrder key_order, Resource *subject, Resource *predicate, Resource *object);

    bool find();
    bool next();

  private:
    bool read();

    TripleTable& table;
    Resource *subject, *predicate, *object;
    TripleOrder key_order;
    std::vector<uint32_t> key_ids;

    int idx;
    uint32_t x_block_no;
    uint32_t y_block_no;

    std::vector<std::string> values;
  };
  friend class BlockScanner;

  bool read(TripleOrder key_order, Resource *subject, Resource *predicate, Resource *object);
  bool readBitVector(TripleOrder key_order, uint32_t subject, uint32_t predicate, uint32_t object, RoaringBitVector& bitvec);

  void write(const Resource& subject, const Resource& predicate, const Resource& object);
  void write(TripleOrder key_order, const Resource& subject, const Resource& predicate, const Resource& object);

  int count(TripleOrder key_order, uint32_t subject, uint32_t predicate, uint32_t object);
  int distinctCount(TripleOrder key_order, uint32_t subject, uint32_t predicate, uint32_t object);

private:
  #pragma pack(push, 1)
  // KVStore
  struct Key {
    uint8_t label;
    uint32_t x;
    uint32_t y;
  };
  struct Segment {
    uint32_t count;
    uint32_t x_distinct_count;
    uint32_t y_distinct_count;
    uint32_t x_first_block_no;
    uint32_t x_last_block_no;
    uint32_t y_first_block_no;
    uint32_t y_last_block_no;
    uint32_t x_index_block_no;
    uint32_t y_index_block_no;
    uint16_t dtype;
    uint16_t dsize;
    char data[1];
  };
  struct Node {
    uint32_t block_no;
    uint32_t next_block_no;
    uint16_t dtype;
    uint16_t dsize;
    char data[1];
  };
  #pragma pack(pop)

  static const uint32_t KEY_SIZE;

  static const uint32_t SEGMENT_MAX_SIZE;
  static const uint32_t SEGMENT_HEADER_SIZE;
  static const uint32_t SEGMENT_DATA_MAX_SIZE;

  static const uint32_t NODE_HEADER_SIZE;
  static const uint32_t NODE_DATA_MAX_SIZE;

  // Segment file
  class HeapFile {
  public:
    HeapFile(const std::string& file_path, BufferManager* buffer_manager);
    ~HeapFile();

    bool open();
    bool close();

    BufferPage* appendNode();
    BufferPage* getNode(uint32_t block_no);
    void prefetchNode(uint32_t block_no);
    void updateNode(BufferPage* page, bool dirty, bool exclusive);

  private:
    #pragma pack(push, 1)
    struct Header {
      uint32_t version;
      uint32_t capacity;
      uint32_t block_size;
      uint32_t num_block;
      uint64_t file_size;
    };
    #pragma pack(pop)

    static const uint32_t HEADER_SIZE;
    static const uint32_t FILE_GROWTH;

    bool create(uint32_t capacity, uint32_t version);

    bool readHeader();
    bool writeHeader();

    std::string file_path;
    RandomRWFile file;
    Header* header;

    BufferManager* buffer_manager;
    Mutex mutex;
  };

  bool readHeader();


  bool writeHeader();


  BufferPage* getNode(uint32_t block_no);
  BufferPage* appendNode();

  bool readSegment(Key &key, Segment* segment);

  bool readNode(uint32_t block_no, Node* node);
  bool writeNode(uint32_t block_no, Node* node);
  bool readData(const Segment* segment, int pos, std::vector<uint32_t>& column);
  void writeData(Segment* segment, int pos, const std::vector<uint32_t>& column);
  bool readBitVector(Segment* segment, ResourcePosition pos, RoaringBitVector& bitvec);
  void writeBitVector(Segment* segment, ResourcePosition pos, const RoaringBitVector& bitvec);

  RocksDBStore kvstore;

  HeapFile data_file;
  HeapFile index_file;
};


#endif
