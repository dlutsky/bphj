#include "bit_set.h"

/* BitSet */

BitSet::BitSet(size_t size_in_bits) : size_in_bits(size_in_bits) {
  size_t buf_size = (size_in_bits + BITS_PER_WORD - 1) / BITS_PER_WORD;
  buffer.resize(buf_size);
}

BitSet::BitSet(const BitSet& other) {
  size_in_bits = other.size_in_bits;
  buffer = other.buffer;
}

BitSet::~BitSet() {}

void BitSet::set(size_t pos) {
  const size_t word_pos = pos / BITS_PER_WORD;
  buffer[word_pos] |= static_cast<uint64_t>(1) << (pos % BITS_PER_WORD);
}

void BitSet::clear(size_t pos) {
  const size_t word_pos = pos / BITS_PER_WORD;
  buffer[word_pos] &= ~(static_cast<uint64_t>(1) << (pos % BITS_PER_WORD));
}

void BitSet::reset() {
  buffer.clear();
}

bool BitSet::test(size_t pos) const {
  const size_t word_pos = pos / BITS_PER_WORD;
  return buffer[word_pos] & static_cast<uint64_t>(1) << (pos % BITS_PER_WORD);
}

bool BitSet::any() const {
  for(size_t i=0; i<buffer.size(); i++) {
    if(buffer[i] != 0) {
      return true;
    }
  }
  return false;
}

BitSet& BitSet::operator=(const BitSet& other) {
  size_in_bits = other.size_in_bits;
  buffer = other.buffer;
  return *this;
}

BitSet BitSet::operator|(const BitSet& other) {
  BitSet bitset(size_in_bits);
  for(size_t i=0; i<bitset.buffer.size(); i++) {
    bitset.buffer[i] = buffer[i] | other.buffer[i];
  }
  return bitset;
}

BitSet& BitSet::operator|=(const BitSet& other) {
  for(size_t i=0; i<buffer.size(); i++) {
    buffer[i] |= other.buffer[i];
  }
  return *this;
}

BitSet BitSet::operator&(const BitSet& other) {
  BitSet bitset(size_in_bits);
  for(size_t i=0; i<bitset.buffer.size(); i++) {
    bitset.buffer[i] = buffer[i] & other.buffer[i];
  }
  return bitset;
}

BitSet& BitSet::operator&=(const BitSet& other) {
  for(size_t i=0; i<buffer.size(); i++) {
    buffer[i] &= other.buffer[i];
  }
  return *this;
}

BitSet BitSet::operator^(const BitSet& other) {
  BitSet bitset(size_in_bits);
  for(size_t i=0; i<bitset.buffer.size(); i++) {
    bitset.buffer[i] = buffer[i] ^ other.buffer[i];
  }
  return bitset;
}

BitSet& BitSet::operator^=(const BitSet& other) {
  for(size_t i=0; i<buffer.size(); i++) {
    buffer[i] ^= other.buffer[i];
  }
  return *this;
}

bool BitSet::operator<(const BitSet& other) const {
  for(size_t i=0; i<buffer.size(); i++) {
    if(buffer[i] < other.buffer[i]) {
      return true;
    } else if(buffer[i] > other.buffer[i]) {
      return false;
    }
  }
  return false;
}


/* BitSet::SetBitIterator */

BitSet::SetBitIterator::SetBitIterator() : buffer(nullptr), pos(0), has_value(false) {}

BitSet::SetBitIterator::SetBitIterator(const BitSet& bitset) : SetBitIterator(bitset, 0) {}

BitSet::SetBitIterator::SetBitIterator(const BitSet& bitset, size_t pos)
  : buffer(&(bitset.buffer)), pos(pos), has_value(false) {
  word_pos = pos / BitSet::BITS_PER_WORD;
  if(word_pos < buffer->size()) {
    word = (*buffer)[word_pos];
    has_value = moveToNext();
  }
}

BitSet::SetBitIterator::SetBitIterator(const SetBitIterator& it)
  : buffer(it.buffer), pos(it.pos), has_value(it.has_value), word(it.word), word_pos(it.word_pos) {}


bool BitSet::SetBitIterator::moveToNext() {
  while(word == 0) {
    if(++word_pos == buffer->size()) {
      break;
    }
    word = (*buffer)[word_pos];
  }
  if(word == 0) {
    return false;
  }
  uint64_t w = word & (~word+1);
  pos = word_pos * BitSet::BITS_PER_WORD + __builtin_popcountl(w-1);
  // pos = word_pos * BitSet::BITS_PER_WORD + std::popcount(w-1);
  word ^= w;
  return true;
}

size_t BitSet::SetBitIterator::operator*() const {
  return pos;
}

BitSet::SetBitIterator& BitSet::SetBitIterator::operator++() {
  has_value = moveToNext();
  return *this;
}

BitSet::SetBitIterator BitSet::SetBitIterator::operator++(int) {
  BitSet::SetBitIterator it(*this);
  has_value = moveToNext();
  return it;
}

bool BitSet::SetBitIterator::operator==(const BitSet::SetBitIterator& other) const {
  if(!has_value) {
    return !other.has_value;
  }
  return other.has_value && pos == other.pos;
}

bool BitSet::SetBitIterator::operator!=(const BitSet::SetBitIterator& other) const {
  if(!has_value) {
    return other.has_value;
  }
  return !other.has_value || pos != other.pos;
}

BitSet::SetBitIterator BitSet::begin() {
  return BitSet::SetBitIterator(*this);
}

BitSet::SetBitIterator& BitSet::end() {
  static BitSet::SetBitIterator e;
  return e;
}
