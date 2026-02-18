#include "datasource.h"
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>

auto get_path(const std::string &file_name) -> std::string
{
  std::filesystem::path cwd(__FILE__);
  return cwd.parent_path().append("data").append("csv").append(file_name);
}

TEST(Datasource, can_infer_schema)
{
  std::string file = get_path("with_header.csv");
  CsvDatasource datasource({}, file);

  auto schema = datasource.schema();
  ASSERT_EQ(schema->fields().size(), 3);
}

TEST(Datasource, handles_csv_with_missing_values) {}
TEST(Datasource, respects_provided_schema) {}
TEST(Datasource, projects_selected_columns) {}
TEST(Datasource, handles_missing_csv_file) {}
TEST(Datasource, handles_invalid_csv_file) {}
