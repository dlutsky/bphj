#ifndef RW_LATCH_H
#define RW_LATCH_H

#include <pthread.h>


class ReadWriteLatch {
public:
  ReadWriteLatch();
  ~ReadWriteLatch();

  void readLock();
  void writeLock();
  void unlock();
  
private:
  pthread_rwlock_t rw_lock;
};



#endif
