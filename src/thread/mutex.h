#ifndef MUTEX_H
#define MUTEX_H

#include <pthread.h>

class Condition;

class Mutex {
public:
  Mutex();
  ~Mutex();

  void lock();
  bool tryLock();
  void unlock();

private:
  friend class Condition;

  pthread_mutex_t mutex;
};


#endif
