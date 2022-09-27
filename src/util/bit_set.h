#ifndef BITSET_H
#define BITSET_H

#include <cstdint>
#include <vector>
#include <sys/types.h>

class BitSet {
public:
  BitSet(size_t size_in_bits);
  BitSet(const BitSet& other);
  ~BitSet();

  void set(size_t pos);
  void clear(size_t pos);
  void reset();

  bool test(size_t pos) const;

  bool any() const;

  BitSet& operator=(const BitSet& other);

  BitSet operator|(const BitSet& other);
  BitSet& operator|=(const BitSet& other);
  BitSet operator&(const BitSet& other);
  BitSet& operator&=(const BitSet& other);
  BitSet operator^(const BitSet& other);
  BitSet& operator^=(const BitSet& other);

  bool operator<(const BitSet& other) const;

  class SetBitIterator {
  public:
    SetBitIterator();
    SetBitIterator(const BitSet& bitset);
    SetBitIterator(const BitSet& bitset, size_t pos);

    SetBitIterator(const SetBitIterator& it);
    size_t operator*() const;
    SetBitIterator& operator++();
    SetBitIterator operator++(int);
    bool operator==(const SetBitIterator& other) const;
    bool operator!=(const SetBitIterator& other) const;

  private:
    bool moveToNext();

    const std::vector<uint64_t> *buffer;

    size_t pos;
    bool has_value;
    uint32_t word;
    size_t word_pos;
  };

  SetBitIterator begin();
  SetBitIterator& end();

private:
  static const size_t BITS_PER_WORD = 8 * sizeof(uint64_t);

  std::vector<uint64_t> buffer;
  size_t size_in_bits;
};


#endif
