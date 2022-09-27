#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <unordered_map>
#include "util/memory_pool.h"

class MemoryPoolTest : public testing::Test {
protected:
};

TEST_F(MemoryPoolTest, iterator1) {
  MemoryPool<int> pool;

  int *a = pool.alloc();
  *a = 5;
  int *b = pool.alloc();
  *b = 7;
  int *c = pool.alloc();
  *c = 9;
  EXPECT_EQ(5, *a);
  EXPECT_EQ(7, *b);
  EXPECT_EQ(9, *c);
  pool.free(b);
  int *d = pool.alloc();
  *d = 11;
  EXPECT_EQ(11, *d);
  EXPECT_NE(7, *b);
  EXPECT_EQ(11, *b);
}

TEST_F(MemoryPoolTest, iterator2) {
  MemoryPool<std::string> pool;

  std::string *a = pool.alloc();
  *a = "a-string";
  std::string *b = pool.alloc();
  *b = "b-string";
  std::string *c = pool.alloc();
  *c = "c-string";
  EXPECT_EQ("a-string", *a);
  EXPECT_EQ("b-string", *b);
  EXPECT_EQ("c-string", *c);
  pool.free(b);
  std::string *d = pool.alloc();
  *d = "d-string";
  EXPECT_EQ("d-string", *d);
  std::string *e = pool.alloc();
  *e = "e-string";
  EXPECT_EQ("e-string", *e);
}

struct TestEntry1 {
  enum Type { A, B, C };
  Type type;
  std::string value;
  TestEntry1() {}
};

TEST_F(MemoryPoolTest, iterator3) {
  MemoryPool<TestEntry1> pool;

  TestEntry1 *a = pool.alloc();
  a->type = TestEntry1::A;
  a->value = "a-value";
  EXPECT_EQ(TestEntry1::A, a->type);
  EXPECT_EQ("a-value", a->value);
  TestEntry1 *b = pool.alloc();
  b->type = TestEntry1::B;
  b->value = "b-value";
  EXPECT_EQ(TestEntry1::B, b->type);
  EXPECT_EQ("b-value", b->value);
}

struct TestEntry2 {
  int key;
  std::string value;
  std::vector<int> arr;
};

TEST_F(MemoryPoolTest, iterator4) {
  MemoryPool<TestEntry2> pool;

  TestEntry2 *a = pool.alloc();
  a->key = 5;
  a->value = "a-value";
  EXPECT_EQ(5, a->key);
  EXPECT_EQ("a-value", a->value);
  TestEntry2 *b = pool.alloc();
  b->key = 7;
  b->value = "b-value";
  EXPECT_EQ(7, b->key);
  EXPECT_EQ("b-value", b->value);
  TestEntry2 *c = pool.alloc();
  c->key = 9;
  c->value = "c-value";
  c->arr.push_back(1);
  c->arr.push_back(2);
  EXPECT_EQ(9, c->key);
  EXPECT_EQ("c-value", c->value);
  EXPECT_EQ(2, c->arr.size());
  EXPECT_EQ(1, c->arr[0]);
  EXPECT_EQ(2, c->arr[1]);
  pool.free(b);
  TestEntry2 *d = pool.alloc();
  d->key = 11;
  d->value = "d-value";
  EXPECT_EQ(11, d->key);
  TestEntry2 *e = pool.alloc();
  e->key = 13;
  e->value = "e-value";
  EXPECT_EQ(13, e->key);
  EXPECT_EQ("e-value", e->value);
}
