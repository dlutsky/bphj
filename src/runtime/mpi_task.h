#ifndef MPI_TASK_H
#define MPI_TASK_H


class MPITask {
public:
  MPITask(Operator* op, MPITask* next);
  ~MPITask();

private:
  MPITask* next;
};



#endif
