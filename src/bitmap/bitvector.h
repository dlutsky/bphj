#ifndef BITVECTOR_H
#define BITVECTOR_H

#include <cstdint>
#include <vector>
#include <sys/types.h>


class BitVector {
public:
  BitVector(size_t size_in_bits);
  BitVector(const BitVector& other);
  ~BitVector();

  void set(size_t pos);
  void clear(size_t pos);
  void clearAll();
  bool test(size_t pos) const;

  BitVector& operator=(const BitVector& other);

  BitVector operator|(const BitVector& other);
  BitVector& operator|=(const BitVector& other);
  BitVector operator&(const BitVector& other);
  BitVector& operator&=(const BitVector& other);
  BitVector operator^(const BitVector& other);
  BitVector& operator^=(const BitVector& other);

  class SetBitIterator {
  public:
    SetBitIterator();
    SetBitIterator(const BitVector& bitvec);
    SetBitIterator(const BitVector& bitvec, size_t pos);

    SetBitIterator(const SetBitIterator& it);
    size_t operator*() const;
    SetBitIterator& operator++();
    SetBitIterator operator++(int);
    bool operator==(const SetBitIterator& other) const;
    bool operator!=(const SetBitIterator& other) const;

  private:
    bool moveToNext();
    static size_t numberOfSetBits(uint32_t word);

    const std::vector<uint32_t> *buffer;

    size_t pos;
    bool has_value;
    uint32_t word;
    size_t word_pos;
  };

  SetBitIterator begin();
  SetBitIterator& end();

private:
  static const size_t BITS_PER_WORD = 8 * sizeof(uint32_t);

  std::vector<uint32_t> buffer;
  size_t size_in_bits;
};




#endif
