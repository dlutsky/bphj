#include "bitvector.h"
#include <iostream>


/* BitVector */

BitVector::BitVector(size_t size_in_bits) : size_in_bits(size_in_bits) {
  size_t buf_size = (size_in_bits + BITS_PER_WORD - 1) / BITS_PER_WORD;
  buffer.resize(buf_size);
}

BitVector::BitVector(const BitVector& other) {
  size_in_bits = other.size_in_bits;
  buffer = other.buffer;
}

BitVector::~BitVector() {}

void BitVector::set(size_t pos) {
  const size_t word_pos = pos / BITS_PER_WORD;
  buffer[word_pos] |= static_cast<uint32_t>(1) << (pos % BITS_PER_WORD);
}

void BitVector::clear(size_t pos) {
  const size_t word_pos = pos / BITS_PER_WORD;
  buffer[word_pos] &= ~(static_cast<uint32_t>(1) << (pos % BITS_PER_WORD));
}

void BitVector::clearAll() {
  buffer.clear();
}

bool BitVector::test(size_t pos) const {
  const size_t word_pos = pos / BITS_PER_WORD;
  return buffer[word_pos] & static_cast<uint32_t>(1) << (pos % BITS_PER_WORD);
}

BitVector& BitVector::operator=(const BitVector& other) {
  size_in_bits = other.size_in_bits;
  buffer = other.buffer;
  return *this;
}

BitVector BitVector::operator|(const BitVector& other) {
  BitVector bitvec(size_in_bits);
  for(size_t i=0; i<bitvec.buffer.size(); i++) {
    bitvec.buffer[i] = buffer[i] | other.buffer[i];
  }
  return bitvec;
}

BitVector& BitVector::operator|=(const BitVector& other) {
  for(size_t i=0; i<buffer.size(); i++) {
    buffer[i] |= other.buffer[i];
  }
  return *this;
}

BitVector BitVector::operator&(const BitVector& other) {
  BitVector bitvec(size_in_bits);
  for(size_t i=0; i<bitvec.buffer.size(); i++) {
    bitvec.buffer[i] = buffer[i] & other.buffer[i];
  }
  return bitvec;
}

BitVector& BitVector::operator&=(const BitVector& other) {
  for(size_t i=0; i<buffer.size(); i++) {
    buffer[i] &= other.buffer[i];
  }
  return *this;
}

BitVector BitVector::operator^(const BitVector& other) {
  BitVector bitvec(size_in_bits);
  for(size_t i=0; i<bitvec.buffer.size(); i++) {
    bitvec.buffer[i] = buffer[i] ^ other.buffer[i];
  }
  return bitvec;
}

BitVector& BitVector::operator^=(const BitVector& other) {
  for(size_t i=0; i<buffer.size(); i++) {
    buffer[i] ^= other.buffer[i];
  }
  return *this;
}


/* BitVector::SetBitIterator */

BitVector::SetBitIterator::SetBitIterator() : buffer(nullptr), pos(0), has_value(false) {}

BitVector::SetBitIterator::SetBitIterator(const BitVector& bitvec) : SetBitIterator(bitvec, 0) {}

BitVector::SetBitIterator::SetBitIterator(const BitVector& bitvec, size_t pos)
  : buffer(&(bitvec.buffer)), pos(pos), has_value(false) {
  word_pos = pos / BitVector::BITS_PER_WORD;
  if(word_pos < buffer->size()) {
    word = (*buffer)[word_pos];
    has_value = moveToNext();
  }
}

BitVector::SetBitIterator::SetBitIterator(const SetBitIterator& it)
  : buffer(it.buffer), pos(it.pos), has_value(it.has_value), word(it.word), word_pos(it.word_pos) {}


bool BitVector::SetBitIterator::moveToNext() {
  while(word == 0) {
    if(++word_pos == buffer->size()) {
      break;
    }
    word = (*buffer)[word_pos];
  }
  if(word == 0) {
    return false;
  }
  uint32_t w = word & (~word+1);
  pos = word_pos * BitVector::BITS_PER_WORD + numberOfSetBits(w-1);
  word ^= w;
  return true;
}

/*
  Counting bits set, in parallel
  https://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel
*/
inline size_t BitVector::SetBitIterator::numberOfSetBits(uint32_t word) {
  word = word - ((word >> 1) & 0x55555555);
  word = (word & 0x33333333) + ((word >> 2) & 0x33333333);
  return (((word + (word >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
}

size_t BitVector::SetBitIterator::operator*() const {
  return pos;
}

BitVector::SetBitIterator& BitVector::SetBitIterator::operator++() {
  has_value = moveToNext();
  return *this;
}

BitVector::SetBitIterator BitVector::SetBitIterator::operator++(int) {
  BitVector::SetBitIterator it(*this);
  has_value = moveToNext();
  return it;
}

bool BitVector::SetBitIterator::operator==(const BitVector::SetBitIterator& other) const {
  if(!has_value) {
    return !other.has_value;
  }
  return other.has_value && pos == other.pos;
}

bool BitVector::SetBitIterator::operator!=(const BitVector::SetBitIterator& other) const {
  if(!has_value) {
    return other.has_value;
  }
  return !other.has_value || pos != other.pos;
}

BitVector::SetBitIterator BitVector::begin() {
  return BitVector::SetBitIterator(*this);
}

BitVector::SetBitIterator& BitVector::end() {
  static BitVector::SetBitIterator e;
  return e;
}
