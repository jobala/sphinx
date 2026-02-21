#include "csv_datasource_iter.h"
#include "datasource.h"
#include "types.h"
#include <any>
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

TEST(Datasource, returns_selected_columns)
{
  std::string file = get_path("with_header.csv");
  CsvDatasource datasource({}, file);
  auto iter = datasource.scan(std::vector<String>{"id", "name"});

  ASSERT_EQ(iter->has_next(), true);

  auto record_batch = iter->next();
  ASSERT_EQ(record_batch.RowCount(), 5);
  ASSERT_EQ(record_batch.ColumnCount(), 2);

  std::vector<std::string> ids{"1", "2", "3", "4", "5"};
  std::vector<std::string> names{"Alice", "Bob", "Carol", "David", "Eve"};

  for (int i = 0; i < record_batch.RowCount(); i++)
  {
    auto user_id = std::any_cast<std::string>(record_batch.GetField(0)->GetValue(i));
    auto user_name = std::any_cast<std::string>(record_batch.GetField(1)->GetValue(i));

    ASSERT_EQ(user_id, ids[i]);
    ASSERT_EQ(user_name, names[i]);
  }
}

TEST(Datasource, handle_missing_value) {}

TEST(Datasource, handle_different_column_types) {}
