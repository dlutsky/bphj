#include <iostream>
#include "command.h"


int main(int argc, char **argv){
  Command cmd = CommandBuilder::parse(argc, argv);
  if(!cmd) {
    exit(1);
  }
  cmd->run();
  return 0;
}
