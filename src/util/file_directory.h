#ifndef FILE_DIRECTORY_H
#define FILE_DIRECTORY_H


#include <string>
#include <memory>
#include <vector>
#include <sys/types.h>
#include <fcntl.h>


class SequentialFileWriter;
class BufferedFileWriter;
class MmapFileReader;

class File {
public:
  File(const std::string& file_path);
  ~File();
  static bool exist(const std::string& file_path);
  static bool create(const std::string& file_path);
  static bool remove(const std::string& file_path);

  bool open(int flags = O_RDONLY);
  bool close();

private:
  friend SequentialFileWriter;
  friend BufferedFileWriter;
  friend MmapFileReader;

  int fd;
  std::string file_path;
};

class Directory {
public:
  static bool exist(const std::string& dir_path);
  static bool create(const std::string& dir_path);
  static bool getFiles(const std::string& dir_path, std::vector<std::string> &files);
  static bool getFiles(const std::string& dir_path, std::vector<std::string> &files, const std::string& pattern);
};



class SequentialFileWriter {
public:
  SequentialFileWriter(const std::string& file_path);
  bool close();

  bool append(const char* data, size_t size);

private:
  std::unique_ptr<File> file;
};

class BufferedFileWriter {
public:
  BufferedFileWriter(const std::string& file_path, size_t buffer_size = DEFAULT_BUFFER_SIZE);
  ~BufferedFileWriter();
  bool close();

  bool append(const char* data, size_t size);
  bool flush();

private:
  bool write(const char* data, size_t &size);

  static size_t DEFAULT_BUFFER_SIZE;

  std::unique_ptr<File> file;
  char* buffer;
  size_t buffer_size;
  size_t buffer_count;
};


class MmapFileReader {
public:
  MmapFileReader(const std::string& file_path);
  ~MmapFileReader();
  bool close();

  const char* begin();
  size_t size();

private:
  std::unique_ptr<File> file;
  struct MmapData;
  std::unique_ptr<MmapData> data;
};


class RandomRWFile {
public:
  RandomRWFile(const std::string& file_path);
  ~RandomRWFile();
  bool create();
  bool open(int flags = O_RDONLY);
  bool close();

  bool prefetch(off64_t offset, size_t size);
  bool read(void *buffer, size_t size, off_t offset);
  bool write(const void *buffer, size_t size, off_t offset);
  bool truncate(off_t length);
  bool flush();

private:
  int fd;
  std::string file_path;
};


#endif
