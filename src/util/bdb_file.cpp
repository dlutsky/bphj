#include <cstring>
#include "bdb_file.h"


BDBFile::BDBFile(const std::string& file_path) : file_path(file_path) {
  file = new Db(NULL, 0);
}

BDBFile::~BDBFile() {
  delete file;
}

bool BDBFile::open() {
  try {
    file->open(
        NULL,
        file_path.c_str(),
        NULL,
        DB_BTREE,
        DB_CREATE,
        0664
    );
    file->cursor(NULL, &cursor, 0);
  } catch (DbException &e) {
    return false;
  }
  return true;
}

bool BDBFile::close() {
  cursor->close();
  return file->close(0) == 0;
}

bool BDBFile::put(const std::string& key, const std::string& value) {
  Dbt key_entry((void*)const_cast<char*>(key.data()), key.size());
  Dbt data_entry((void*)const_cast<char*>(value.data()), value.size());
  if(file->put(NULL, &key_entry, &data_entry, 0) != 0) {
    return false;
  }
  return true;
}

bool BDBFile::flush() {
  return file->sync(0) == 0;
}

bool BDBFile::get(const std::string& key, std::string* value) {
  Dbt key_entry((void*)const_cast<char*>(key.data()), key.size());
  Dbt data_entry;
  if(cursor->get(&key_entry, &data_entry, DB_SET) != 0) {
    return false;
  }
  value->assign((char*)data_entry.get_data(), data_entry.get_size());
  return true;
}

bool BDBFile::getNext(std::string* value) {
  Dbt key_entry;
  Dbt data_entry;
  if(cursor->get(&key_entry, &data_entry, DB_NEXT) != 0) {
    return false;
  }
  value->assign((char*)data_entry.get_data(), data_entry.get_size());
  return true;
}

bool BDBFile::getNext(const std::string& key, std::string* value) {
  Dbt key_entry;
  Dbt data_entry;
  if(cursor->get(&key_entry, &data_entry, DB_NEXT_DUP) != 0) {
    return false;
  }
  if (memcmp(key_entry.get_data(), key.data(), key.size()) != 0) {
    return false;
  }
  value->assign((char*)data_entry.get_data(), data_entry.get_size());
  return true;
}
