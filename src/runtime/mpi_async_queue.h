#ifndef MPI_ASYNC_QUEUE_H
#define MPI_ASYNC_QUEUE_H

#include <cstddef>
#include "mpi.h"

/**
 * MPIAsyncQueue is an async multi-producer-single-consumer queue.
 */
class MPIAsyncQueue {
public:
  MPIAsyncQueue(MPI_Comm comm);
  ~MPIAsyncQueue();

  template <size_t N>
  void send(const FixedBytes<N>& data);
  int take(FixedBytes<N>& data);

private:
  void init();

  struct RemotePointer {
    int rank;
    MPI_Aint disp;
    void* local_pointer;
  };
  struct Node {
    RemotePointer next;
    char* data;
  };
  MPI_Datatype pointer_type;
  MPI_Datatype node_type;

  int rank;
};

#endif
