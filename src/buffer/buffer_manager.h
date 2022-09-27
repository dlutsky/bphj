#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <cstdint>
#include <map>
#include "buffer_page.h"
#include "util/file_directory.h"
#include "common/constants.h"
#include "thread/mutex.h"


class BufferManager {
public:
  BufferManager(int buffer_capacity);
  ~BufferManager();

  BufferPage* getBufferPage(RandomRWFile *file, uint32_t block_no, bool exclusive);
  BufferPage* allocBufferPage(RandomRWFile *file, uint32_t block_no, bool exclusive);
  void unfixBufferPage(BufferPage* page, bool dirty, bool exclusive);
  void flushBufferPage(BufferPage* page, bool exclusive);
  void flushFile(RandomRWFile *file, bool exclusive);

private:
  struct PageID {
    RandomRWFile *file;
    uint32_t block_no;

    PageID(RandomRWFile *file, uint32_t block_no);

    bool operator==(const PageID& other) const;
    bool operator<(const PageID& other) const;
  };

  int buffer_size;
  int buffer_capacity;

  std::map<PageID, BufferPage*> hash_table;
  BufferPage* lru_cache_head;
  BufferPage* lru_cache_tail;

  Mutex mutex;
};




#endif
