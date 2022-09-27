#include <string.h>
#include <sys/stat.h>
#include <dirent.h>
#include <system_error>
#include <cstdio>
#include <unistd.h>
#include <sys/mman.h>
#include "file_directory.h"
#include "string_util.h"


/* File */

File::File(const std::string& file_path)
  : fd(-1), file_path(file_path) {}

File::~File() {
  if(fd >= 0) {
    close();
  }
}

bool File::exist(const std::string& file_path) {
  struct stat buf;
  return stat(file_path.c_str(), &buf) == 0;
}

bool File::create(const std::string& file_path) {
  int fd = ::creat(file_path.c_str(), 0644);
  if(fd < 0) {
    return false;
  }
  return true;
}

bool File::remove(const std::string& file_path) {
  if(::remove(file_path.c_str()) != 0) {
    return false;
  }
  return true;
}

bool File::open(int flags /*=O_RDONLY*/) {
  if(fd >= 0) {
    return true;
  }
  while (fd < 0) {
    fd = ::open(file_path.c_str(), flags, 0644);
    if(fd < 0) {
      if(errno == EINTR) {
        continue;
      }
      return false;
    }
  }
  return true;
}

bool File::close() {
  if(::close(fd) < 0) {
    return false;
  }
  fd = -1;
  return true;
}


/* Directory */

bool Directory::exist(const std::string& dir_path) {
  struct stat status;
  if (stat(dir_path.c_str(), &status) != 0) {
    return false;
  }
  return S_ISDIR(status.st_mode);
}

bool Directory::create(const std::string& dir_path) {
  if(mkdir(dir_path.c_str(), 0755) == -1) {
    return false;
  }
  return true;
}

bool Directory::getFiles(const std::string& dir_path, std::vector<std::string> &files) {
  DIR *dp;
  struct dirent *dirp;
  if((dp  = opendir(dir_path.c_str())) == NULL) {
    return false;
  }

  while ((dirp = readdir(dp)) != NULL) {
    std::string file(dirp->d_name);
    if (strcmp(file.c_str(), ".") != 0 && strcmp(file.c_str(), "..") != 0) {
      files.push_back(dir_path + "/" + file);
    }
  }
  closedir(dp);
  return true;
}

bool Directory::getFiles(const std::string& dir_path, std::vector<std::string> &files, const std::string& pattern) {
  DIR *dp;
  struct dirent *dirp;
  if((dp  = opendir(dir_path.c_str())) == NULL) {
    return false;
  }

  while ((dirp = readdir(dp)) != NULL) {
    std::string file(dirp->d_name);
    if (strcmp(file.c_str(), ".") == 0 || strcmp(file.c_str(), "..") == 0) {
      continue;
    }
    if(StringUtil::isMatch(file, pattern)) {
      files.push_back(dir_path + "/" + file);
    }
  }
  closedir(dp);
  return true;
}



/* SequentialFileWriter */

SequentialFileWriter::SequentialFileWriter(const std::string& file_path) : file(new File(file_path)) {
  file->open(O_WRONLY|O_CREAT);
}

bool SequentialFileWriter::close() {
  return file->close();
}

bool SequentialFileWriter::append(const char* data, size_t size) {
  size_t done = 0;
  while (size != 0) {
    ssize_t wsize = ::write(file->fd, data+done, size);
    if (wsize < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    size -= wsize;
    done += wsize;
  }
  return true;
}


/* BufferedFileWriter */

size_t BufferedFileWriter::DEFAULT_BUFFER_SIZE = 8192;

BufferedFileWriter::BufferedFileWriter(const std::string& file_path, size_t buffer_size /*=DEFAULT_BUFFER_SIZE*/)
  : file(new File(file_path)), buffer_size(buffer_size) {
  file->open(O_WRONLY|O_CREAT);
  buffer = new char[buffer_size];
  buffer_count = 0;
}

BufferedFileWriter::~BufferedFileWriter() {}

bool BufferedFileWriter::close() {
  if(buffer_count) {
    flush();
  }
  delete [] buffer;
  return file->close();
}

bool BufferedFileWriter::append(const char* data, size_t size) {
  if(buffer_count + size > buffer_size) {
    flush();
  }
  if(size > buffer_size) {
    return write(data, size);
  }
  memcpy(buffer+buffer_count, data, size);
  buffer_count += size;
  return true;
}

bool BufferedFileWriter::flush() {
  write(buffer, buffer_count);
  buffer_count = 0;
  return true;
}

bool BufferedFileWriter::write(const char* data, size_t &size) {
  size_t done = 0;
  while (size != 0) {
    ssize_t wsize = ::write(file->fd, data+done, size);
    if (wsize < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    size -= wsize;
    done += wsize;
  }
  return true;
}



/* MmapFileReader */

struct MmapFileReader::MmapData {
  void* mapping;
  char* begin;
  size_t size;
};

MmapFileReader::MmapFileReader(const std::string& file_path)
  : file(new File(file_path)), data(new MmapData()) {
  file->open();
  //file->open(O_RDONLY | O_NONBLOCK);
  data->size =  ::lseek(file->fd, 0, SEEK_END);
  data->mapping = ::mmap(nullptr, static_cast<size_t>(data->size), PROT_READ, MAP_SHARED, file->fd, 0);
  if(data->mapping == MAP_FAILED) {
    file->close();
  }
  data->begin = static_cast<char*>(data->mapping);
}

MmapFileReader::~MmapFileReader() {}

bool MmapFileReader::close() {
  ::munmap(data->mapping, data->size);
  data.reset(nullptr);
  return file->close();
}

const char* MmapFileReader::begin() {
  if(!data) {
    return nullptr;
  }
  return data->begin;
}

size_t MmapFileReader::size() {
  if(!data) {
    return 0;
  }
  return data->size;
}




/* RandomRWFile */

RandomRWFile::RandomRWFile(const std::string& file_path)
  : fd(-1), file_path(file_path) {}

RandomRWFile::~RandomRWFile() {
  if(fd >= 0) {
    close();
  }
}

bool RandomRWFile::create() {
  fd = ::creat(file_path.c_str(), 0644);
  if(fd < 0) {
    return false;
  }
  return true;
}

bool RandomRWFile::open(int flags /*=O_RDONLY*/) {
  if(fd >= 0) {
    return true;
  }
  while (fd < 0) {
    fd = ::open(file_path.c_str(), flags, 0644);
    if(fd < 0) {
      if(errno == EINTR) {
        continue;
      }
      return false;
    }
  }
  return true;
}

bool RandomRWFile::close() {
  if(::close(fd) < 0) {
    return false;
  }
  fd = -1;
  return true;
}

bool RandomRWFile::prefetch(off64_t offset, size_t size) {
  ssize_t rsize = ::readahead(fd, offset, size);
  if (rsize < 0) {
    return false;
  }
  return true;
}

bool RandomRWFile::read(void *buffer, size_t size, off_t offset) {
  while (size > 0) {
    ssize_t rsize = ::pread(fd, buffer, size, offset);
    if (rsize <= 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    buffer += rsize;
    offset += rsize;
    size -= rsize;
  }
  return true;
}

bool RandomRWFile::write(const void *buffer, size_t size, off_t offset) {
  size_t done = 0;
  while (size != 0) {
    ssize_t wsize = ::pwrite(fd, buffer+done, size, offset);
    if (wsize < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    size -= wsize;
    offset += wsize;
    done += wsize;
  }
  return true;
}

bool RandomRWFile::truncate(off_t length) {
  return ftruncate(fd, length) == 0;
}

bool RandomRWFile::flush() {
  return fsync(fd) == 0;
}
