#ifndef PARTITIONER_H


class PartitionFrame {
public:
  uint32_t getPartitionId();

private:
  uint32_t ptn_id;
};

class Partitioner {
public:
  Partitioner();
  ~Partitioner();

  void build();

  PartitionFrame getPartition(uint32_t predicate);

private:
  std::map<unit32_t, PartitionFrame*> ptn_map;
  std::vector<Partition> ptns;
};



#endif
