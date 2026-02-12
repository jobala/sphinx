#include "types.h"
#include <optional>
#include <string>
#include <vector>

using Schema = arrow::Schema;
using String = std::string;

class Datasource {
public:
  virtual ~Datasource() = default;
  virtual arrow::Schema schema() = 0;
  virtual RecordBatch scan(std::vector<String> project) = 0;
};

class CsvDatasource : public Datasource {
  std::optional<Schema> schema_;

public:
  CsvDatasource(const std::optional<Schema> &schema);

  Schema schema() override;
  RecordBatch scan(std::vector<String> project) override;
};
