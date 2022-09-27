#include "dictionary.h"

Dictionary::Dictionary(const std::string& store_path)
  : str2id(store_path + "/str2id.bdb"), id2str(store_path + "/id2str.bdb"),
    meta_file_path(store_path + "/dict_meta.info"), meta_file(meta_file_path) {}

Dictionary::~Dictionary() {}

bool Dictionary::open() {
  if(!File::exist(meta_file_path)) {
    if(!createMetafile(META_FILE_SIZE)) {
      return false;
    }
  }
  // file.open();
  if(!meta_file.open(O_RDWR)) {
    return false;
  }
  meta_data = reinterpret_cast<Metadata*>(new char[META_FILE_SIZE]);
  if(!readMetadata()) {
    return false;
  }

  str2id.open();
  id2str.open();
  return true;
}

bool Dictionary::close() {
  writeMetadata();
  delete[] reinterpret_cast<char*>(meta_data);
  meta_file.close();

  str2id.close();
  id2str.close();
  return true;
}

uint32_t Dictionary::append(const std::string& str) {
  std::string id_str;
  if(str2id.get(str, &id_str)) {
    return std::stoi(id_str);
  }
  ++meta_data->last_seq_id;
  ++meta_data->total_count;
  str2id.put(str, std::to_string(meta_data->last_seq_id));
  id2str.put(std::to_string(meta_data->last_seq_id), str);
  return meta_data->last_seq_id;
}

bool Dictionary::lookup(const std::string& str, uint32_t *id) {
  std::string id_str;
  if(!str2id.get(str, &id_str)) {
    return false;
  }
  *id = std::stoi(id_str);
  return true;
}

bool Dictionary::lookupById(uint32_t id, std::string *str) {
  std::string id_str = std::to_string(id);
  if(!id2str.get(id_str, str)) {
    return false;
  }
  return true;
}

uint64_t Dictionary::count() {
  return meta_data->total_count;
}

const uint64_t Dictionary::INIT_ID = 1;
const uint32_t Dictionary::META_FILE_SIZE = sizeof(uint64_t) * 2;

bool Dictionary::createMetafile(uint32_t capacity) {
  meta_file.open(O_WRONLY|O_CREAT);
  meta_data = reinterpret_cast<Metadata*>(new char[META_FILE_SIZE]);
  meta_data->total_count = 0;
  meta_data->last_seq_id = INIT_ID;
  meta_file.truncate(META_FILE_SIZE);
  writeMetadata();
  delete[] reinterpret_cast<char*>(meta_data);
  meta_file.close();
  return true;
}

bool Dictionary::readMetadata() {
  meta_file.read(reinterpret_cast<char*>(meta_data), META_FILE_SIZE, 0);
  return true;
}

bool Dictionary::writeMetadata() {
  meta_file.write(reinterpret_cast<const char*>(meta_data), META_FILE_SIZE, 0);
  return true;
}
