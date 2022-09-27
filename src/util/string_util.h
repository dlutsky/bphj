#ifndef STRING_UTIL_H
#define STRING_UTIL_H

#include <sstream>
#include <vector>


class StringUtil {
public:
  static std::vector<std::string> split(const std::string& str, char delimiter);
  static bool isMatch(const std::string& s, const std::string& p);
};

#endif
