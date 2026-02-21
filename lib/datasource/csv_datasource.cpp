
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "datasource.h"
#include "types.h"

CsvDatasource::CsvDatasource(std::optional<std::shared_ptr<Schema>> schema, const String &file_name)
    : schema_(std::move(schema)), file_(file_name)
{
  schema_ = schema_.value_or(this->infer_schema());
};

std::shared_ptr<Schema> CsvDatasource::infer_schema()
{
  std::string line;
  std::getline(file_, line);
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::stringstream sstream(line);
  std::string field_name;

  while (std::getline(sstream, field_name, ','))
  {
    auto field = arrow::field(field_name, ArrowTypes::StringType);
    fields.push_back(field);
  }

  return arrow::schema(fields);
}

std::shared_ptr<Schema> CsvDatasource::schema() { return schema_.value(); }

DatasourceIterator CsvDatasource::scan(const std::vector<String> &projection)
{
  std::cout << projection.front();
  return std::make_unique<CsvDatasourceIterator<RecordBatch>>(file_, final_schema_);
}
