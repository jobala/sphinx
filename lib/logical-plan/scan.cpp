#include "scan.h"
#include "logical_plan.h"
#include <format>
#include <memory>
#include <vector>

Scan::Scan(const std::string &path, const std::vector<std::string> &projection, Datasource &datasource)
    : path_(path), projection_(projection), datasource_(datasource)
{
  schema_ = derive_schema();
}

auto Scan::schema() -> std::shared_ptr<arrow::Schema> { return schema_; }

auto Scan::derive_schema() -> std::shared_ptr<arrow::Schema>
{
  if (projection_.empty())
  {
    return datasource_.schema();
  }
  return datasource_.create_final_schema(projection_);
}

auto Scan::children() -> std::vector<std::shared_ptr<LogicalPlan>>
{
  return std::vector<std::shared_ptr<LogicalPlan>>{};
}

auto Scan::to_string() -> std::string
{
  if (projection_.empty())
  {
    return std::format("Scan {}: projection=None", path_);
  }
  return std::format("Scan {}: projection={}", path_, projection_);
}
