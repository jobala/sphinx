#pragma once
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "csv_datasource_iter.h"
#include "types.h"

using Schema = arrow::Schema;
using String = std::string;

using DatasourceIterator = std::unique_ptr<Iterator<RecordBatch>>;

class Datasource
{
public:
  virtual ~Datasource() = default;
  virtual std::shared_ptr<Schema> schema() = 0;
  virtual DatasourceIterator scan(const std::vector<String> &project) = 0;
};

class CsvDatasource : public Datasource
{
  std::optional<std::shared_ptr<Schema>> schema_;
  std::shared_ptr<Schema> final_schema_;
  std::shared_ptr<Schema> infer_schema();
  std::ifstream file_;

public:
  CsvDatasource(std::optional<std::shared_ptr<Schema>> schema, const String &file_name);

  std::shared_ptr<Schema> schema() override;
  DatasourceIterator scan(const std::vector<String> &project) override;
};
