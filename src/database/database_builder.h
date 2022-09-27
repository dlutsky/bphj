#ifndef DATABASE_BUILDER_H
#define DATABASE_BUILDER_H

#include <string>
#include <vector>
#include <cstdint>
#include "util/file_directory.h"
#include "storage/dictionary.h"



class DatabaseBuilder {
public:
  DatabaseBuilder(const std::string& db_name);
  bool buildFromRDFFiles(std::vector<std::string>& rdf_files);

private:
  bool encodeRDFFile(const std::string& rdf_file, Dictionary &dict, BufferedFileWriter& triple_file_writer);
  bool buildStoragesForPSO(const std::string& triple_file);
  bool buildStoragesForPOS(const std::string& triple_file);

  std::string store_path;
  std::string db_name;

  uint64_t triple_count;
  uint32_t num_res;
};



#endif
