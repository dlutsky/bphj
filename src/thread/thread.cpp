#include "thread.h"


Thread::Thread(Runnable* runnable, bool detached) : runnable(runnable), detached(detached) {}

bool Thread::start() {
  if(pthread_create(&tid, NULL, threadMain, (void*)runnable) != 0) {
    return false;
  }
  if(detached) {
    pthread_detach(tid);
  }
  return true;
}

void Thread::join() {
  pthread_join(tid, NULL);
}

long Thread::threadID() {
  union {
    pthread_t tid;
    unsigned long id;
  } thread;
  thread.id = 0;
  thread.tid = pthread_self();
  return thread.id;
}

void* Thread::threadMain(void* arg) {
  Runnable* runnable = static_cast<Runnable*>(arg);
  runnable->run();
}
