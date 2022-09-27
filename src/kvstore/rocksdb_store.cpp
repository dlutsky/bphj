#include "rocksdb_store.h"
#include "util/file_directory.h"



SstFile::SstFile(const std::string& file_path)
  : file_path(file_path), options(), sst_file_writer(rocksdb::EnvOptions(), options) {}

bool SstFile::open() {
  rocksdb::Status status = sst_file_writer.Open(file_path);
  return status.ok();
}

bool SstFile::append(const std::string& key, const std::string& value) {
  rocksdb::Status status = sst_file_writer.Put(key, value);
  return status.ok();
}

bool SstFile::close() {
  rocksdb::Status status = sst_file_writer.Finish();
  return status.ok();
}

bool SstFile::discard() {
  return File::remove(file_path);
}


RocksDBStore::RocksDBStore(const std::string& store_path) : store_path(store_path), db(nullptr) {}

RocksDBStore::~RocksDBStore() {
  if(db != nullptr) {
    delete db;
  }
}

bool RocksDBStore::open() {
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(options, store_path, &db);
  return status.ok();
}

bool RocksDBStore::close() {
  delete db;
  db = nullptr;
}

bool RocksDBStore::get(const std::string& key, std::string* value) {
  rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, value);
  return status.ok();
}

bool RocksDBStore::multiGet(const std::vector<std::string>& keys, std::vector<std::string>* values) {
  std::vector<rocksdb::Slice> _keys;
  for(int i=0; i<keys.size(); i++) {
    _keys.push_back(rocksdb::Slice(keys[i]));
  }
  // std::vector<rocksdb::Status> status = db->MultiGet(rocksdb::ReadOptions(), _keys, values);
  db->MultiGet(rocksdb::ReadOptions(), _keys, values);
  return true;
}

bool RocksDBStore::put(const std::string& key, const std::string& value) {
  rocksdb::Status status = db->Put(rocksdb::WriteOptions(), key, value);
  return status.ok();
}

bool RocksDBStore::ingestExternalFile(const SstFile& file) {
  rocksdb::IngestExternalFileOptions fileOptions;
  rocksdb::Status status = db->IngestExternalFile({file.file_path}, fileOptions);
  return status.ok();
}

RocksDBStore::TransactionImpl::TransactionImpl(rocksdb::DB *db) : db(db) {}

void RocksDBStore::TransactionImpl::put(const std::string& key, const std::string& value) {
  batch.Put(key, value);
}

bool RocksDBStore::TransactionImpl::commit() {
  rocksdb::Status status = db->Write(rocksdb::WriteOptions(), &batch);
  batch.Clear();
  return status.ok();
}

RocksDBStore::Transaction RocksDBStore::createTransaction() {
  return std::make_shared<RocksDBStore::TransactionImpl>(db);
}
