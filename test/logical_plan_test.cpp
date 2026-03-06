#include <filesystem>
#include <format>
#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "datasource.h"
#include "scan.h"

auto get_path(const std::string &file_name) -> std::string
{
  std::filesystem::path cwd(__FILE__);
  return cwd.parent_path().append("data").append("csv").append(file_name).string();
}

TEST(LogicalPlan, scan_uses_full_schema)
{
  std::string file = get_path("with_header.csv");
  CsvDatasource datasource({}, file);
  std::vector<std::string> projection{};

  Scan scan(file, projection, datasource);
  std::vector<std::string> headers{"id", "name", "score"};
  std::shared_ptr<arrow::Schema> scan_schema = scan.schema();

  ASSERT_EQ(headers.size(), int(scan_schema->fields().size()));
  for (int i = 0; i < int(scan_schema->fields().size()); i++)
  {
    ASSERT_EQ(headers[i], scan_schema->field(i)->name());
  }
}

TEST(LogicalPlan, scan_uses_projected_schema)
{
  std::string file = get_path("with_header.csv");
  CsvDatasource datasource({}, file);
  std::vector<std::string> projection{"name", "score"};

  Scan scan(file, projection, datasource);
  std::vector<std::string> headers{"name", "score"};
  std::shared_ptr<arrow::Schema> scan_schema = scan.schema();

  ASSERT_EQ(2, int(scan_schema->fields().size()));
  for (int i = 0; i < int(scan_schema->fields().size()); i++)
  {
    ASSERT_EQ(headers[i], scan_schema->field(i)->name());
  }
}

TEST(LogicalPlan, scan_to_string)
{
  std::string file = get_path("with_header.csv");
  CsvDatasource datasource({}, file);
  std::vector<std::string> projection{};
  Scan scan(file, projection, datasource);

  ASSERT_TRUE(scan.to_string().contains("projection=None"));

  projection = {"name", "score"};
  Scan scan2(file, projection, datasource);

  ASSERT_TRUE(scan2.to_string().contains(std::format("projection={}", projection)));
}
