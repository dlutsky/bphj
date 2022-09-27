#include <string.h>
#include "database_builder.h"
#include "config.h"
#include "util/sorter.h"
#include "util/static_vector.h"
#include "parser/turtle_parser.h"
#include "storage/triple_table.h"

#include <iostream>

static const size_t ENCODED_TRIPLE_SIZE = sizeof(uint32_t) * 3;
static const size_t FULL_TRIPLE_SIZE = sizeof(uint32_t) * 3 + sizeof(uint16_t) * 3;

#pragma pack(push, 1)
struct EncodedTriple {
  uint32_t sid;
  uint32_t pid;
  uint32_t oid;
  size_t size() const {
    return ENCODED_TRIPLE_SIZE;
  }
};
struct FullTriple {
  uint32_t sid;
  uint32_t pid;
  uint32_t oid;
  uint16_t ssize;
  uint16_t psize;
  uint16_t osize;
  char data[1];
  size_t size() const {
    return FULL_TRIPLE_SIZE + ssize + psize + osize;
  }
};
#pragma pack(pop)

struct PtrCompareByPsoOrder {
  inline bool operator() (const EncodedTriple* triple1, const EncodedTriple* triple2) {
    if(triple1->pid<triple2->pid || (triple1->pid==triple2->pid && triple1->sid<triple2->sid) || (triple1->pid==triple2->pid && triple1->sid==triple2->sid && triple1->oid<triple2->oid)) {
      return true;
    }
    return false;
  }
};

struct PtrCompareByPosOrder {
  inline bool operator() (const EncodedTriple* triple1, const EncodedTriple* triple2) {
    if(triple1->pid<triple2->pid || (triple1->pid==triple2->pid && triple1->oid<triple2->oid) || (triple1->pid==triple2->pid && triple1->oid==triple2->oid && triple1->sid<triple2->sid)) {
      return true;
    }
    return false;
  }
};


DatabaseBuilder::DatabaseBuilder(const std::string& db_name) : db_name(db_name) {
  Config config;
  store_path = config.getParam(ConfigKey::STORE_PATH) + "/" + db_name;
  triple_count = 0;
}

bool DatabaseBuilder::buildFromRDFFiles(std::vector<std::string>& rdf_files) {
  std::string triple_file = store_path + "/triple_file.temp";

  BufferedFileWriter writer(triple_file);
  Dictionary dict(store_path);
  dict.open();

  for(std::vector<std::string>::iterator it = rdf_files.begin(), end = rdf_files.end(); it != end; ++it) {
    if(!encodeRDFFile(*it, dict, writer)) {
      return false;
    }
  }

  num_res = dict.count();
  writer.close();
  dict.close();

  if(!buildStoragesForPSO(triple_file)) {
    return false;
  }

  if(!buildStoragesForPOS(triple_file)) {
    return false;
  }

  File::remove(triple_file);

  return true;
}

bool DatabaseBuilder::encodeRDFFile(const std::string& rdf_file, Dictionary &dict, BufferedFileWriter& writer) {
  std::cout<<"Parsing "<<rdf_file<<std::endl;
  TurtleFileParser ttl_parser(rdf_file);

  std::string subject;
  std::string predicate;
  std::string object;
  bool full = false;
  if(full) {
    FullTriple triple;
    while(ttl_parser.parse(subject, predicate, object)){
      triple.sid = dict.append(subject);
      triple.ssize = subject.length();
      triple.pid = dict.append(predicate);
      triple.psize = predicate.length();
      triple.oid = dict.append(object);
      triple.osize = object.length();
      
      writer.append(reinterpret_cast<const char*>(&triple), FULL_TRIPLE_SIZE);
      writer.append(subject.c_str(), subject.length());
      writer.append(predicate.c_str(), predicate.length());
      writer.append(object.c_str(), object.length());
      
      ++triple_count;
    }
  } else {
    EncodedTriple triple;
    while(ttl_parser.parse(subject, predicate, object)){
      triple.sid = dict.append(subject);
      triple.pid = dict.append(predicate);
      triple.oid = dict.append(object);
      
      writer.append(reinterpret_cast<const char*>(&triple), ENCODED_TRIPLE_SIZE);
      
      ++triple_count;
    }
  }
  

  return true;
}

bool DatabaseBuilder::buildStoragesForPSO(const std::string& triple_file) {
  std::cout<<"Build PSO index "<<std::endl;
  std::string pso_file = store_path + "/pso_file.temp";
  Sorter::sort<EncodedTriple>(triple_file, pso_file, PtrCompareByPsoOrder());

  BufferManager buffer_manager(1000);


  TripleTable triple_table(store_path, "triple_table", &buffer_manager);
  if(!triple_table.open()) {
    return false;
  }

  MmapFileReader reader(pso_file);
  StaticVector<EncodedTriple> pool(reader.begin(), reader.size());
  typename StaticVector<EncodedTriple>::Iterator iter = pool.begin(), end = pool.end();

  Resource predicate;
  Resource subject1;
  Resource object1;

  Resource subject2;
  Resource object2;

  uint32_t prev_pid = 0;
  uint32_t prev_sub = 0;
  uint32_t prev_obj = 0;

  uint32_t cur_pid = iter->pid;
  uint32_t cur_sub = iter->sid;
  for(; iter != end; ++iter) {
    if(cur_pid != iter->pid) {
      predicate.id = cur_pid;
      triple_table.write(TripleOrder::P, subject1, predicate, object1);
      subject1.column.clear();
      object1.column.clear();

      subject2.id = cur_sub;
      triple_table.write(TripleOrder::SP, subject2, predicate, object2);
      object2.column.clear();

      cur_pid = iter->pid;
      cur_sub = iter->sid;
    } else if(cur_sub != iter->sid) {
      predicate.id = cur_pid;
      subject2.id = cur_sub;
      triple_table.write(TripleOrder::SP, subject2, predicate, object2);
      object2.column.clear();

      cur_sub = iter->sid;
    }

    if(prev_pid == cur_pid && prev_sub == iter->sid && prev_obj == iter->oid) {
      continue;
    }

    subject1.column.push_back(iter->sid);
    object1.column.push_back(iter->oid);

    object2.column.push_back(iter->oid);

    prev_pid = cur_pid;
    prev_sub = iter->sid;
    prev_obj = iter->oid;

    if(subject1.column.size() >= 2000) {
      predicate.id = cur_pid;
      triple_table.write(TripleOrder::P, subject1, predicate, object1);
      subject1.column.clear();
      object1.column.clear();
    }
  }
  predicate.id = cur_pid;
  triple_table.write(TripleOrder::P, subject1, predicate, object1);
  subject2.id = cur_sub;
  triple_table.write(TripleOrder::SP, subject2, predicate, object2);

  reader.close();

  triple_table.close();

  File::remove(pso_file);
  return true;
}

bool DatabaseBuilder::buildStoragesForPOS(const std::string& triple_file) {
  std::cout<<"Build POS index "<<std::endl;
  std::string pos_file = store_path + "/pos_file.temp";
  Sorter::sort<EncodedTriple>(triple_file, pos_file, PtrCompareByPosOrder());

  BufferManager buffer_manager(1000);

  TripleTable triple_table(store_path, "triple_table", &buffer_manager);
  if(!triple_table.open()) {
    return false;
  }

  MmapFileReader reader(pos_file);
  StaticVector<EncodedTriple> pool(reader.begin(), reader.size());
  typename StaticVector<EncodedTriple>::Iterator iter = pool.begin(), end = pool.end();

  Resource predicate;
  Resource subject;
  Resource object;

  uint32_t prev_pid = 0;
  uint32_t prev_sub = 0;
  uint32_t prev_obj = 0;

  uint32_t cur_pid = iter->pid;
  uint32_t cur_obj = iter->oid;
  for(; iter != end; ++iter) {
    if(cur_pid != iter->pid) {
      predicate.id = cur_pid;
      object.id = cur_obj;
      triple_table.write(TripleOrder::OP, subject, predicate, object);
      subject.column.clear();

      cur_pid = iter->pid;
      cur_obj = iter->oid;
    } else if(cur_obj != iter->oid) {
      predicate.id = cur_pid;
      object.id = cur_obj;
      triple_table.write(TripleOrder::OP, subject, predicate, object);
      subject.column.clear();

      cur_obj = iter->oid;
    }

    if(prev_pid == cur_pid && prev_sub == iter->sid && prev_obj == iter->oid) {
      continue;
    }

    subject.column.push_back(iter->sid);

    prev_pid = cur_pid;
    prev_sub = iter->sid;
    prev_obj = iter->oid;
  }
  predicate.id = cur_pid;
  object.id = cur_obj;
  triple_table.write(TripleOrder::OP, subject, predicate, object);

  reader.close();

  triple_table.close();

  File::remove(pos_file);
  return true;
}
