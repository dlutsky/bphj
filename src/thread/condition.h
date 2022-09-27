#ifndef CONDITION_H
#define CONDITION_H

#include <pthread.h>
#include "mutex.h"


class Condition {
public:
  Condition();
  ~Condition();

  void wait(Mutex& mutex);
  void notifyOne();
  void notifyAll();

private:
  pthread_cond_t condition;
};





#endif
