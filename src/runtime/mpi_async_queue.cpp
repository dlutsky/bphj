#include "mpi_queue.h"

MPIAsyncQueue::MPIAsyncQueue(MPI_Comm comm) {
}

MPIAsyncQueue::~MPIAsyncQueue() {

}

void MPIAsyncQueue::init() {
  int block_lengths[] = {1, 1};
  MPI_Aint displacements[] = {offsetof(RemotePointer, rank), offsetof(RemotePointer, disp)};
  MPI_Datatype types[] = {MPI_INT, MPI_AINT};
  MPI_Type_create_struct(2, block_lengths, displacements, types, &pointer_type);
}

void MPIAsyncQueue::send(const FixedBytes<N>& data) {

}

int MPIAsyncQueue::take(FixedBytes<N>& data) {
  
}
