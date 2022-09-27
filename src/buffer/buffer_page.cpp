#include "buffer_page.h"

BufferPage::BufferPage(RandomRWFile *file, uint32_t block_no)
  : file(file), block_no(block_no), dirty(false), prev_page(nullptr), next_page(nullptr) {}

BufferPage::~BufferPage() {}

uint32_t BufferPage::getBlockNo() {
  return block_no;
}

char* BufferPage::getBlockData() {
  return block_data;
}
