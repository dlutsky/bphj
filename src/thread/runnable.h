#ifndef RUNNABLE_H
#define RUNNABLE_H



class Runnable {
public:
  virtual ~Runnable() {};

  virtual void run() = 0;
};



#endif
