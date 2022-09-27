#ifndef BUFFER_PAGE_H
#define BUFFER_PAGE_H

#include <cstdint>
#include "thread/rw_latch.h"
#include "common/constants.h"
#include "util/file_directory.h"


class BufferManager;

class BufferPage {
public:
  BufferPage(RandomRWFile *file, uint32_t block_no);
  ~BufferPage();

  uint32_t getBlockNo();
  char* getBlockData();

private:
  friend class BufferManager;

  ReadWriteLatch latch;
  bool dirty;
  uint32_t block_no;
  RandomRWFile* file;
  BufferPage* prev_page;
  BufferPage* next_page;
  //void* block_data;
  uint64_t start_pos;
  char block_data[BLOCK_SIZE];
};


#endif
