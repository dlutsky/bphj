#include "rw_latch.h"


ReadWriteLatch::ReadWriteLatch() {
  pthread_rwlock_init(&rw_lock, nullptr);
}

ReadWriteLatch::~ReadWriteLatch() {
  pthread_rwlock_destroy(&rw_lock);
}

void ReadWriteLatch::readLock() {
  pthread_rwlock_rdlock(&rw_lock);
}

void ReadWriteLatch::writeLock() {
  pthread_rwlock_wrlock(&rw_lock);
}

void ReadWriteLatch::unlock() {
  pthread_rwlock_unlock(&rw_lock);
}
