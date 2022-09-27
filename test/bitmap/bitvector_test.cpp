#include <string>
#include <gtest/gtest.h>
#include "bitmap/bitvector.h"


class BitVectorTest : public testing::Test {
protected:
};

TEST_F(BitVectorTest, iterator1) {
  BitVector bitvec(70);
  int bits[4] = {5, 27, 55, 68};
  bitvec.set(bits[0]);
  bitvec.set(bits[1]);
  bitvec.set(bits[2]);
  bitvec.set(bits[3]);
  int i=0;
  for(BitVector::SetBitIterator it = bitvec.begin(); it != bitvec.end(); ++it) {
    EXPECT_EQ(bits[i++], *it);
  }
}
