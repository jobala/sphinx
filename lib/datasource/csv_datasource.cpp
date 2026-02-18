
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "datasource.h"
#include "types.h"

std::shared_ptr<Schema> CsvDatasource::infer_schema()
{
  std::string line;
  std::getline(file_, line);

  std::cout << line << "\n";
  return arrow::schema({});
}

std::shared_ptr<Schema> CsvDatasource::schema() { return final_schema_; }

DatasourceIterator CsvDatasource::scan(const std::vector<String> &projection)
{
  std::cout << projection.front();
  return std::make_unique<CsvDatasourceIterator<RecordBatch>>(&file_, final_schema_);
}
