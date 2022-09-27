#ifndef SORTER_H
#define SORTER_H

#include <string>
#include <vector>
#include <algorithm>
#include <cstdint>
#include "file_directory.h"
#include "static_vector.h"


class Sorter {
public:
  template <typename T, typename PtrCompare>
  static void sort(std::string in_file, std::string out_file, PtrCompare comp);

private:
  const static uint32_t MEM_LIMIT;

  template <typename T>
  struct Range {
    typename StaticVector<T>::Iterator from;
    typename StaticVector<T>::Iterator to;

    Range(const typename StaticVector<T>::Iterator& from, const typename StaticVector<T>::Iterator& to) : from(from), to(to) {}
  };

  template <typename T, typename PtrCompare>
  struct RangeGreater {
    PtrCompare comp;

    RangeGreater(PtrCompare comp) : comp(comp) {}

    inline bool operator() (Range<T>& range1, Range<T>& range2) {
      return !comp(&(range1.from), &(range2.from));
    }
  };

  template <typename T>
  static void spool(BufferedFileWriter& writer, const T* item);

  template <typename T>
  static void spool(BufferedFileWriter& writer, std::vector<const T*>& items);
};



const uint32_t Sorter::MEM_LIMIT = sizeof(void*)*(1<<27);

template <typename T, typename PtrCompare>
void Sorter::sort(std::string in_file, std::string out_file, PtrCompare comp) {
  MmapFileReader reader(in_file);
  std::string inter_file = in_file + ".inter";
  BufferedFileWriter inter_writer(inter_file);

  std::vector<uint64_t> offsets;
  uint64_t offset = 0;
  StaticVector<T> pool(reader.begin(), reader.size());
  typename StaticVector<T>::Iterator iter = pool.begin(), end = pool.end();
  while(iter != end) {
    std::vector<const T*> items;
    uint32_t size = 0;
    while(size < MEM_LIMIT && iter != end) {
      items.push_back(&iter);
      size += iter->size();
      ++iter;
    }
    std::sort(items.begin(), items.end(), comp);
    spool(inter_writer, items);
    offsets.push_back(offset);
    offset += size;
  }
  reader.close();
  inter_writer.close();


  MmapFileReader inter_reader(inter_file);
  BufferedFileWriter writer(out_file);

  StaticVector<T> inter_pool(inter_reader.begin(), inter_reader.size());
  std::vector<Range<T>> ranges;
  int i;
  for(i=1; i<offsets.size(); i++) {
    ranges.push_back(Range<T>(inter_pool.find(offsets[i-1]), inter_pool.find(offsets[i])));
  }
  ranges.push_back(Range<T>(inter_pool.find(offsets[i-1]), inter_pool.end()));


  RangeGreater<T, PtrCompare> range_greater(comp);
  // min heap
  std::make_heap(ranges.begin(), ranges.end(), range_greater);
  while(!ranges.empty()) {
    std::pop_heap(ranges.begin(), ranges.end(), range_greater);

    spool(writer, &(ranges.back().from));
    ++ranges.back().from;
    
    if(ranges.back().from!=ranges.back().to) {
      std::push_heap(ranges.begin(), ranges.end(), range_greater);
    }
    else {
      ranges.pop_back();
    }
  }

  inter_reader.close();
  writer.close();
  File::remove(inter_file);
}

template <typename T>
void Sorter::spool(BufferedFileWriter& writer, const T* item) {
  writer.append(reinterpret_cast<const char*>(item), item->size());
}

template <typename T>
void Sorter::spool(BufferedFileWriter& writer, std::vector<const T*>& items) {
  typename std::vector<const T*>::iterator iter = items.begin(), end = items.end();
  while(iter != end) {
    writer.append(reinterpret_cast<const char*>(*iter), (*iter)->size());
    ++iter;
  }
}

#endif
