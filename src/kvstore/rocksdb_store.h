#ifndef ROCKSDB_H
#define ROCKSDB_H

#include <memory>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/sst_file_writer.h"


class RocksDBStore;

class SstFile {
public:
  SstFile(const std::string& file_path);
  bool open();
  bool append(const std::string& key, const std::string& value);
  bool close();
  bool discard();

private:
  std::string file_path;
  rocksdb::Options options;
  rocksdb::SstFileWriter sst_file_writer;

  friend class RocksDBStore;
};



class RocksDBStore {
public:
  RocksDBStore(const std::string& store_path);
  ~RocksDBStore();
  bool open();
  bool close();
  bool get(const std::string& key, std::string* value);
  bool multiGet(const std::vector<std::string>& keys, std::vector<std::string>* values);
  bool put(const std::string& key, const std::string& value);
  bool ingestExternalFile(const SstFile& file);

  class TransactionImpl {
  public:
    explicit TransactionImpl(rocksdb::DB *db);

    void put(const std::string& key, const std::string& value);
    bool commit();

  private:
    rocksdb::DB *db;
    rocksdb::WriteBatch batch;
  };
  typedef std::shared_ptr<TransactionImpl> Transaction;

  Transaction createTransaction();

private:
  std::string store_path;
  rocksdb::Options options;
  rocksdb::DB *db;
};



#endif
