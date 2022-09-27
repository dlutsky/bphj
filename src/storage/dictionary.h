#ifndef DICTIONARY_H
#define DICTIONARY_H

#include <string>
#include <cstdint>
#include "util/bdb_file.h"
#include "util/file_directory.h"


class DictionaryBuilder;

class Dictionary {
public:
  Dictionary(const std::string& store_path);
  ~Dictionary();

  bool open();
  bool close();

  uint32_t append(const std::string& str);

  bool lookup(const std::string& str, uint32_t *id);
  bool lookupById(uint32_t id, std::string *str);
  uint64_t count();

private:
  #pragma pack(push, 1)
  struct Metadata {
    uint64_t total_count;
    uint64_t last_seq_id;
  };
  #pragma pack(pop)

  friend DictionaryBuilder;

  static const uint64_t INIT_ID;
  static const uint32_t META_FILE_SIZE;

  bool createMetafile(uint32_t capacity);
  bool readMetadata();
  bool writeMetadata();

  BDBFile str2id;
  BDBFile id2str;

  std::string meta_file_path;
  RandomRWFile meta_file;
  Metadata* meta_data;
};


#endif
