#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <map>

class ConfigKey {
public:
  const static std::string STORE_PATH;
};

class Config {
public:
  static void loadConfig(std::string config_filepath);
  const std::string& getParam(const std::string& key) const;

private:
  const static std::string DEFAULT_CONFIG_FILEPATH;
  const static std::string DEFAULT_STORE_PATH;

  static std::map<std::string, std::string> config_map;
  static struct StaticConstructor {
    StaticConstructor() {
      config_map[ConfigKey::STORE_PATH] = DEFAULT_STORE_PATH;
      loadConfig(DEFAULT_CONFIG_FILEPATH);
    }
  } static_constructor;
};


#endif
