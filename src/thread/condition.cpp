#include "condition.h"


Condition::Condition() {
  pthread_cond_init(&condition, 0);
}

Condition::~Condition() {
  pthread_cond_destroy(&condition);
}

void Condition::wait(Mutex& mutex) {
  pthread_cond_wait(&condition, &(mutex.mutex));
}

void Condition::notifyOne() {
  pthread_cond_signal(&condition);
}

void Condition::notifyAll() {
  pthread_cond_broadcast(&condition);
}
