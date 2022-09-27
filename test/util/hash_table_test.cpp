#include <string>
#include <gtest/gtest.h>
#include "util/hash_table.h"


class HashTableTest : public testing::Test {
protected:
};

class Hasher {
public:
  static size_t hash1 (int key, size_t table_size) {
    return key & (table_size-1);
  }

  static size_t hash2 (int key, size_t table_size) {
    return (key << 32) & (table_size-1);
  }
};

TEST_F(HashTableTest, iterator1) {
  HashTable<int, int, Hasher> hash_table(4);
  hash_table.insert(3,5);
  hash_table.insert(6,9);
  hash_table.insert(3,7);
  hash_table.insert(1,4);
  HashTable<int, int, Hasher>::ValueIterator iter = hash_table.lookup(3);
  EXPECT_EQ(7, *iter);
  ++iter;
  EXPECT_EQ(5, *iter);
  iter = hash_table.lookup(6);
  EXPECT_EQ(9, *iter);
  iter = hash_table.lookup(1);
  EXPECT_EQ(4, *iter);
}
