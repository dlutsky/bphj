#include "buffer_manager.h"


BufferManager::BufferManager(int buffer_capacity)
  : buffer_size(0), buffer_capacity(buffer_capacity), lru_cache_head(nullptr), lru_cache_tail(nullptr) {}

BufferManager::~BufferManager() {
  for (std::map<PageID, BufferPage*>::iterator iter=hash_table.begin(); iter!=hash_table.end(); ++iter) {
    BufferPage* page = (*iter).second;
    if(page->dirty) {
      page->file->write(page->block_data, BLOCK_SIZE, page->block_no * BLOCK_SIZE);
    }
    delete page;
  }
  this->lru_cache_head = nullptr;
  this->lru_cache_tail = nullptr;
}

BufferPage* BufferManager::getBufferPage(RandomRWFile *file, uint32_t block_no, bool exclusive) {
  PageID pageId(file, block_no);
  BufferPage* page = nullptr;
  if(exclusive) {
    mutex.lock();
  }
  std::map<PageID, BufferPage*>::iterator iter = hash_table.find(pageId);
  if(iter != hash_table.end()) {
    page = (*iter).second;

    if(page->prev_page != nullptr) {
      page->prev_page->next_page = page->next_page;
    } else if(page == this->lru_cache_head) {
      this->lru_cache_head = page->next_page;
      if(this->lru_cache_head != nullptr) {
        this->lru_cache_head->prev_page = nullptr;
      }
    }
    if(page->next_page != nullptr) {
      page->next_page->prev_page = page->prev_page;
    } else if(page == this->lru_cache_tail) {
      this->lru_cache_tail = page->prev_page;
      if(this->lru_cache_tail != nullptr) {
        this->lru_cache_tail->next_page = nullptr;
      }
    }

    page->next_page = nullptr;
    page->prev_page = nullptr;
  } else {
    if(this->buffer_size < this->buffer_capacity) {
      page = new BufferPage(file, block_no);
      this->buffer_size++;
    } else {
      page = this->lru_cache_tail;
      this->lru_cache_tail = this->lru_cache_tail->prev_page;
      this->lru_cache_tail->next_page = nullptr;
      hash_table.erase(PageID(page->file, page->block_no));
      if(page->dirty) {
        page->file->write(page->block_data, BLOCK_SIZE, page->block_no * BLOCK_SIZE);
        page->dirty = false;
      }
      page->file = file;
      page->block_no = block_no;
    }

    page->next_page = nullptr;
    page->prev_page = nullptr;
    hash_table[pageId] = page;
    page->file->read(page->block_data, BLOCK_SIZE, page->block_no * BLOCK_SIZE);
  }
  if(exclusive) {
    mutex.unlock();
  }
  return page;
}

BufferPage* BufferManager::allocBufferPage(RandomRWFile *file, uint32_t block_no, bool exclusive) {
  PageID pageId(file, block_no);
  BufferPage* page = nullptr;
  if(exclusive) {
    mutex.lock();
  }
  if(this->buffer_size < this->buffer_capacity) {
    page = new BufferPage(file, block_no);
    this->buffer_size++;
  } else {
    page = this->lru_cache_tail;
    this->lru_cache_tail = this->lru_cache_tail->prev_page;
    this->lru_cache_tail->next_page = nullptr;
    hash_table.erase(PageID(page->file, page->block_no));
    if(page->dirty) {
      page->file->write(page->block_data, BLOCK_SIZE, page->block_no * BLOCK_SIZE);
      //page->file->flush();
      page->dirty = false;
    }

    page->file = file;
    page->block_no = block_no;
  }
  page->next_page = nullptr;
  page->prev_page = nullptr;
  hash_table[pageId] = page;
  page->file->read(page->block_data, BLOCK_SIZE, page->block_no * BLOCK_SIZE);
  if(exclusive) {
    mutex.unlock();
  }
  return page;
}

void BufferManager::unfixBufferPage(BufferPage* page, bool dirty, bool exclusive) {
  if(exclusive) {
    mutex.lock();
  }
  page->dirty = dirty;

  page->next_page = this->lru_cache_head;
  page->prev_page = nullptr;
  if(this->lru_cache_head != nullptr) {
    this->lru_cache_head->prev_page = page;
  }
  this->lru_cache_head = page;

  if(this->lru_cache_tail == nullptr) {
    this->lru_cache_tail = page;
  }
  if(exclusive) {
    mutex.unlock();
  }
}

void BufferManager::flushBufferPage(BufferPage* page, bool exclusive) {
  if(exclusive) {
    mutex.lock();
  }
  if(page->dirty) {
    page->file->write(page->block_data, BLOCK_SIZE, page->block_no * BLOCK_SIZE);
    page->dirty = false;
  }
  if(exclusive) {
    mutex.unlock();
  }
}

void BufferManager::flushFile(RandomRWFile *file, bool exclusive) {
  if(exclusive) {
    mutex.lock();
  }
  for (std::map<PageID, BufferPage*>::iterator iter=hash_table.begin(); iter!=hash_table.end(); ++iter) {
    BufferPage* page = (*iter).second;
    if(page->file==file && page->dirty) {
      page->file->write(page->block_data, BLOCK_SIZE, page->block_no * BLOCK_SIZE);
      page->dirty = false;
    }
  }
  if(exclusive) {
    mutex.unlock();
  }
}

BufferManager::PageID::PageID(RandomRWFile *file, uint32_t block_no)
  : file(file), block_no(block_no) {}

bool BufferManager::PageID::operator==(const PageID& other) const {
  return (file==other.file) && (block_no==other.block_no);
}

bool BufferManager::PageID::operator<(const PageID& other) const {
  return (file<other.file) || ((file==other.file) && (block_no<other.block_no));
}
