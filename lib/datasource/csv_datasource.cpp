
#include <algorithm>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "csv_datasource_iter.h"
#include "datasource.h"
#include "types.h"

CsvDatasource::CsvDatasource(std::optional<std::shared_ptr<Schema>> schema, const String &file_name)
    : schema_(std::move(schema)), file_(file_name)
{
  schema_ = schema_.value_or(this->infer_schema());
};

auto CsvDatasource::infer_schema() -> std::shared_ptr<Schema>
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

auto CsvDatasource::schema() -> std::shared_ptr<Schema> { return schema_.value(); }

auto CsvDatasource::scan(const std::vector<String> &projection) -> DatasourceIterator
{
  final_schema_ = create_final_schema(projection);
  return std::make_unique<CsvDatasourceIterator<RecordBatch>>(file_, final_schema_);
}

auto CsvDatasource::create_final_schema(const std::vector<String> &projection) -> std::shared_ptr<Schema>
{
  std::vector<std::shared_ptr<arrow::Field>> projected_fields;
  if (!schema_.has_value())
  {
    throw std::runtime_error("error creating final schema");
  }

  auto schema = schema_.value();
  for (int i = 0; i < int(schema->fields().size()); i++)
  {
    auto field = schema->fields()[i];
    if (std::ranges::contains(projection, field->name()))
    {
      auto meta = std::unordered_map<std::string, std::string>{{"pos", std::to_string(i)}};
      auto metadata = std::make_shared<arrow::KeyValueMetadata>(meta);
      auto field_with_meta = field->WithMetadata(metadata);
      projected_fields.push_back(field_with_meta);
    }
  }

  return arrow::schema(projected_fields);
}
