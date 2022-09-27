#ifndef BDB_FILE_H
#define BDB_FILE_H

#include <string>
#include <db_cxx.h>


class BDBFile {
public:
  BDBFile(const std::string& file_path);
  ~BDBFile();

  bool open();
  bool close();

  bool put(const std::string& key, const std::string& value);
  bool flush();

  bool get(const std::string& key, std::string* value);
  bool getNext(std::string* value);
  bool getNext(const std::string& key, std::string* value);
private:
  Db* file;
  std::string file_path;

  Dbc* cursor;
};




#endif
