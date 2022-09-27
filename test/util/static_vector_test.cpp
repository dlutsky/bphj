#include <string>
#include <string.h>
#include <sys/types.h>
#include <gtest/gtest.h>
#include "util/static_vector.h"
#include "util/file_directory.h"


class StaticVectorTest : public testing::Test {
protected:
};

static const size_t TEST_ENTRY_SIZE = sizeof(uint32_t) + sizeof(uint8_t);

#pragma pack(push, 1)
struct TestEntry {
  uint32_t key;
  uint8_t dsize;
  char data[1];
  size_t size() const {
    return TEST_ENTRY_SIZE + dsize;
  }
};
#pragma pack(pop)

TEST_F(StaticVectorTest, iterator1) {
  char data[1024];
  size_t size = 0;
  TestEntry *entry = reinterpret_cast<TestEntry*>(new char[25]);
  entry->key = 1;
  entry->dsize = 3;
  strncpy(entry->data, "abc", 3);
  memcpy(data + size, reinterpret_cast<char*>(entry), entry->size());
  size += entry->size();
  entry->key = 2;
  entry->dsize = 3;
  strncpy(entry->data, "abc", 3);
  memcpy(data + size, reinterpret_cast<char*>(entry), entry->size());
  size += entry->size();
  entry->key = 3;
  entry->dsize = 3;
  strncpy(entry->data, "abc", 3);
  memcpy(data + size, reinterpret_cast<char*>(entry), entry->size());
  size += entry->size();
  delete[] reinterpret_cast<char*>(entry);


  StaticVector<TestEntry> list(data, size);
  StaticVector<TestEntry>::Iterator iter = list.begin();
  EXPECT_EQ(1, iter->key);
  EXPECT_EQ(3, iter->dsize);
  EXPECT_EQ(8, iter->size());
  std::string str(iter->data, iter->dsize);
  EXPECT_EQ(0, strcmp("abc", str.c_str()));
  ++iter;
  EXPECT_EQ(2, iter->key);
  ++iter;
  EXPECT_EQ(3, iter->key);
  ++iter;
  EXPECT_EQ(false, iter != list.end());
}

TEST_F(StaticVectorTest, iterator2) {
  char data[1024];
  size_t size = 0;
  TestEntry entry;
  entry.key = 1;
  entry.dsize = 3;
  memcpy(data + size, reinterpret_cast<char*>(&entry), TEST_ENTRY_SIZE);
  size += TEST_ENTRY_SIZE;
  memcpy(data + size, "abc", 3);
  size += entry.dsize;
  entry.key = 2;
  entry.dsize = 3;
  memcpy(data + size, reinterpret_cast<char*>(&entry), TEST_ENTRY_SIZE);
  size += TEST_ENTRY_SIZE;
  memcpy(data + size, "abc", 3);
  size += entry.dsize;
  entry.key = 3;
  entry.dsize = 3;
  memcpy(data + size, reinterpret_cast<char*>(&entry), TEST_ENTRY_SIZE);
  size += TEST_ENTRY_SIZE;
  memcpy(data + size, "abc", 3);
  size += entry.dsize;


  StaticVector<TestEntry> list(data, size);
  StaticVector<TestEntry>::Iterator iter = list.begin();
  EXPECT_EQ(1, iter->key);
  EXPECT_EQ(3, iter->dsize);
  EXPECT_EQ(8, iter->size());
  std::string str(iter->data, iter->dsize);
  EXPECT_EQ(0, strcmp("abc", str.c_str()));
  ++iter;
  EXPECT_EQ(2, iter->key);
  ++iter;
  EXPECT_EQ(3, iter->key);
  ++iter;
  EXPECT_EQ(false, iter != list.end());
}

TEST_F(StaticVectorTest, iterator3) {
  std::string temp_file = "test/data/list.temp";
  SequentialFileWriter writer(temp_file);
  size_t size = 0;
  TestEntry entry;
  entry.key = 1;
  entry.dsize = 3;
  writer.append(reinterpret_cast<char*>(&entry), TEST_ENTRY_SIZE);
  writer.append("abc", 3);
  size += entry.size();
  entry.key = 2;
  entry.dsize = 3;
  writer.append(reinterpret_cast<char*>(&entry), TEST_ENTRY_SIZE);
  writer.append("abc", 3);
  size += entry.size();
  entry.key = 3;
  entry.dsize = 3;
  writer.append(reinterpret_cast<char*>(&entry), TEST_ENTRY_SIZE);
  writer.append("abc", 3);
  size += entry.size();
  writer.close();

  MmapFileReader reader(temp_file);
  EXPECT_EQ(size, reader.size());
  StaticVector<TestEntry> list(reader.begin(), reader.size());
  StaticVector<TestEntry>::Iterator iter = list.begin();
  EXPECT_EQ(1, iter->key);
  EXPECT_EQ(3, iter->dsize);
  EXPECT_EQ(8, iter->size());
  std::string str(iter->data, iter->dsize);
  EXPECT_EQ(0, strcmp("abc", str.c_str()));
  ++iter;
  EXPECT_EQ(2, iter->key);
  ++iter;
  EXPECT_EQ(3, iter->key);
  ++iter;
  EXPECT_EQ(false, iter != list.end());
  reader.close();
}
