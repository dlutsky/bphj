#include <stdexcept>
#include "roaring_bitvector.h"



RoaringBitVector::RoaringBitVector() {
  roaring_bitmap_init_cleared(&roaring);
}

RoaringBitVector::RoaringBitVector(const std::vector<uint32_t>& values) : RoaringBitVector() {
  roaring_bitmap_add_many(&roaring, values.size(), values.data());
}

RoaringBitVector::~RoaringBitVector() {
  if (!(roaring.high_low_container.flags & ROARING_FLAG_FROZEN)) {
    roaring_bitmap_clear(&roaring);
  } else {
    roaring_bitmap_free((roaring_bitmap_t *)((char *)roaring.high_low_container.containers - sizeof(roaring_bitmap_t)));
  }
}

RoaringBitVector::RoaringBitVector(const RoaringBitVector& other) : RoaringBitVector() {
  if (!roaring_bitmap_overwrite(&roaring, &other.roaring)) {
    throw std::runtime_error("failed roaring_bitmap_overwrite in constructor");
  }
  roaring_bitmap_set_copy_on_write(&roaring, roaring_bitmap_get_copy_on_write(&other.roaring));
}

void RoaringBitVector::add(uint32_t value) {
  roaring_bitmap_add(&roaring, value);
}

void RoaringBitVector::add(const std::vector<uint32_t>& values) {
  roaring_bitmap_add_many(&roaring, values.size(), values.data());
}

uint64_t RoaringBitVector::cardinality() const {
  return roaring_bitmap_get_cardinality(&roaring);
}

bool RoaringBitVector::contains(uint32_t value) const {
  return roaring_bitmap_contains(&roaring, value);
}

uint64_t RoaringBitVector::rank(uint32_t value) const {
  return roaring_bitmap_rank(&roaring, value);
}

RoaringBitVector& RoaringBitVector::operator=(const RoaringBitVector& other) {
  roaring_bitmap_clear(&roaring);
  if (!roaring_bitmap_overwrite(&roaring, &other.roaring)) {
    throw std::runtime_error("failed roaring_bitmap_overwrite in constructor");
  }
  roaring_bitmap_set_copy_on_write(&roaring, roaring_bitmap_get_copy_on_write(&other.roaring));
  return *this;
}

RoaringBitVector RoaringBitVector::operator|(const RoaringBitVector& other) {
  roaring_bitmap_t *r = roaring_bitmap_or(&roaring, &other.roaring);
  if (r == NULL) {
    throw std::runtime_error("failed materalization in or");
  }
  return RoaringBitVector(r);
}

RoaringBitVector& RoaringBitVector::operator|=(const RoaringBitVector& other) {
  roaring_bitmap_or_inplace(&roaring, &other.roaring);
  return *this;
}

RoaringBitVector RoaringBitVector::operator&(const RoaringBitVector& other) {
  roaring_bitmap_t *r = roaring_bitmap_and(&roaring, &other.roaring);
  if (r == NULL) {
    throw std::runtime_error("failed materalization in and");
  }
  return RoaringBitVector(r);
}

RoaringBitVector& RoaringBitVector::operator&=(const RoaringBitVector& other) {
  roaring_bitmap_and_inplace(&roaring, &other.roaring);
  return *this;
}

RoaringBitVector RoaringBitVector::operator^(const RoaringBitVector& other) {
  roaring_bitmap_t *r = roaring_bitmap_xor(&roaring, &other.roaring);
  if (r == NULL) {
    throw std::runtime_error("failed materalization in xor");
  }
  return RoaringBitVector(r);
}

RoaringBitVector& RoaringBitVector::operator^=(const RoaringBitVector& other) {
  roaring_bitmap_xor_inplace(&roaring, &other.roaring);
  return *this;
}

bool RoaringBitVector::serialize(const RoaringBitVector& bitvec, ByteBuffer& buf, bool portable) {
  if (portable) {
    size_t buf_size = roaring_bitmap_portable_size_in_bytes(&bitvec.roaring);
    buf.reserve(buf_size);
    buf.resize(roaring_bitmap_portable_serialize(&bitvec.roaring, buf.data()));
  }
  else {
    size_t buf_size = roaring_bitmap_size_in_bytes(&bitvec.roaring);
    buf.reserve(buf_size);
    buf.resize(roaring_bitmap_serialize(&bitvec.roaring, buf.data()));
  }
  return true;
}

bool RoaringBitVector::deserialize(RoaringBitVector& bitvec, const ByteBuffer& buf, bool portable) {
  roaring_bitmap_clear(&bitvec.roaring);
  roaring_bitmap_t *r = portable ? roaring_bitmap_portable_deserialize_safe(buf.data(), buf.size()) : roaring_bitmap_deserialize(buf.data());
  if (r == NULL) {
    throw std::runtime_error("failed alloc while reading");
  }
  bitvec.roaring = *r;
  free(r);
  return true;
}

RoaringBitVector::SetBitIterator::SetBitIterator() {
  iter.container_index = INT32_MAX;
  iter.has_value = false;
  iter.current_value = UINT32_MAX;
}

RoaringBitVector::SetBitIterator::SetBitIterator(const RoaringBitVector &bitvec, bool exhausted /*= false*/) {
  if (exhausted) {
    iter.parent = &bitvec.roaring;
    iter.container_index = INT32_MAX;
    iter.has_value = false;
    iter.current_value = UINT32_MAX;
  }
  else {
    roaring_init_iterator(&bitvec.roaring, &iter);
  }
}

uint32_t RoaringBitVector::SetBitIterator::operator*() const {
  return iter.current_value;
}

RoaringBitVector::SetBitIterator& RoaringBitVector::SetBitIterator::operator++() {
  roaring_advance_uint32_iterator(&iter);
  return *this;
}

RoaringBitVector::SetBitIterator RoaringBitVector::SetBitIterator::operator++(int) {
  SetBitIterator other(*this);
  roaring_advance_uint32_iterator(&iter);
  return other;
}

bool RoaringBitVector::SetBitIterator::operator==(const RoaringBitVector::SetBitIterator& other) const {
  return iter.current_value == other.iter.current_value && iter.has_value == other.iter.has_value;
}

bool RoaringBitVector::SetBitIterator::operator!=(const RoaringBitVector::SetBitIterator& other) const {
  return iter.current_value != other.iter.current_value || iter.has_value != other.iter.has_value;
}

RoaringBitVector::SetBitIterator RoaringBitVector::begin() {
  return SetBitIterator(*this);
}

RoaringBitVector::SetBitIterator& RoaringBitVector::end() {
  static RoaringBitVector::SetBitIterator e(*this, true);
  return e;
}

RoaringBitVector::RoaringBitVector(roaring_bitmap_t *other) {
  roaring = *other;
  free(other);
}
