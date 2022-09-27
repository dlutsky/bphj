#include "config.h"
#include "util/file_directory.h"
#include "yaml-cpp/yaml.h"


const std::string ConfigKey::STORE_PATH = "store_path";


const std::string Config::DEFAULT_CONFIG_FILEPATH = "/etc/grove/init.conf";
const std::string Config::DEFAULT_STORE_PATH = ".";

std::map<std::string, std::string> Config::config_map;
Config::StaticConstructor Config::static_constructor;

void Config::loadConfig(std::string config_filepath) {
  if(!File::exist(config_filepath)) {
    return;
  }
  YAML::Node config = YAML::LoadFile(config_filepath);
  if(config["db.location"]) {
    config_map[DEFAULT_STORE_PATH] = config["db.location"].as<std::string>();
  }
}

const std::string& Config::getParam(const std::string& key) const {
  if(!config_map.count(key)) {
    return "";
  }
  return config_map[key];
}
