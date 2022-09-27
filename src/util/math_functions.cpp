#include <cmath>
#include <algorithm>
#include "math_functions.h"


int MathFunctions::cantorPairing(int a, int b) {
  return ((a+b)*(a+b+1)/2) + b;
}

int MathFunctions::unorderedPairing(int a, int b) {
  int c = std::max(a, b);
  return c*(c+1)/2+std::min(a,b);
}
