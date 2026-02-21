#include "datasource.h"
#include "types.h"
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

auto get_path(const std::string &file_name) -> std::string
{
  std::filesystem::path cwd(__FILE__);
  return cwd.parent_path().append("data").append("csv").append(file_name);
}

TEST(Datasource, infers_schema_from_csv_headers)
{
  std::string file = get_path("with_header.csv");
  CsvDatasource datasource({}, file);

  auto schema = datasource.schema();
  ASSERT_EQ(schema->fields().size(), 3);

  std::vector<std::string> csv_headers{"id", "name", "score"};
  for (int i = 0; i < static_cast<int>(schema->fields().size()); i++)
  {
    auto arrow_field = schema->fields()[i];
    ASSERT_EQ(csv_headers[i], arrow_field->name());
  }
}

TEST(Datasource, uses_provided_schema)
{
  auto name = arrow::field("name", ArrowTypes::StringType);
  auto age = arrow::field("age", ArrowTypes::Int16Type);
  std::vector<std::shared_ptr<arrow::Field>> fields{name, age};

  auto schema = arrow::schema(fields);
  std::string file = get_path("with_header.csv");
  CsvDatasource datasource(schema, file);

  auto datasource_schema = datasource.schema();
  for (int i = 0; i < int(schema->fields().size()); i++)
  {
    ASSERT_EQ(datasource_schema->fields()[i]->name(), schema->fields()[i]->name());
  }
}

TEST(Datasource, projects_selected_columns) {}
TEST(Datasource, handles_missing_csv_file) {}
TEST(Datasource, handles_invalid_csv_file) {}
