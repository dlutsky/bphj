#ifndef ROARING_BITVECTOR_H
#define ROARING_BITVECTOR_H

#include <string>
#include <vector>
#include <memory>
#include <cstdint>
#include <roaring/roaring.h>
#include "util/byte_buffer.h"

class RoaringBitVector {
public:
  RoaringBitVector();
  RoaringBitVector(const std::vector<uint32_t>& values);
  ~RoaringBitVector();

  RoaringBitVector(const RoaringBitVector& other);

  void add(uint32_t value);
  void add(const std::vector<uint32_t>& values);

  uint64_t cardinality() const;

  bool contains(uint32_t value) const;
  uint64_t rank(uint32_t value) const;

  RoaringBitVector& operator=(const RoaringBitVector& other);

  RoaringBitVector operator|(const RoaringBitVector& other);
  RoaringBitVector& operator|=(const RoaringBitVector& other);
  RoaringBitVector operator&(const RoaringBitVector& other);
  RoaringBitVector& operator&=(const RoaringBitVector& other);
  RoaringBitVector operator^(const RoaringBitVector& other);
  RoaringBitVector& operator^=(const RoaringBitVector& other);

  static bool serialize(const RoaringBitVector& bitvec, ByteBuffer& buf, bool portable = true);
  static bool deserialize(RoaringBitVector& bitvec, const ByteBuffer& buf, bool portable = true);

  class SetBitIterator {
  public:
    SetBitIterator();
    SetBitIterator(const RoaringBitVector &bitvec, bool exhausted = false);

    uint32_t operator*() const;
    SetBitIterator& operator++();
    SetBitIterator operator++(int);
    bool operator==(const SetBitIterator& other) const;
    bool operator!=(const SetBitIterator& other) const;

  private:
    roaring_uint32_iterator_t iter;
  };

  SetBitIterator begin();
  SetBitIterator& end();

private:
  RoaringBitVector(roaring_bitmap_t *other);
  
  roaring_bitmap_t roaring;
};




#endif
