#ifndef THREAD_H
#define THREAD_H

#include <pthread.h>
#include "runnable.h"


class Thread {
public:
  Thread(Runnable* runnable, bool detached = true);

  bool start();
  void join();

  static long threadID();

private:
  static void* threadMain(void* arg);

  pthread_t tid;
  Runnable* runnable;
  bool detached;
};


#endif
