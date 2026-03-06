#pragma once
#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>

#include "datasource.h"
#include "logical_plan.h"

class Scan : public LogicalPlan
{
  std::string path_;
  std::vector<std::string> projection_;
  std::shared_ptr<arrow::Schema> schema_;
  Datasource &datasource_;

public:
  Scan(const std::string &path, const std::vector<std::string> &projection, Datasource &datasource);
  std::shared_ptr<arrow::Schema> schema() override;
  std::shared_ptr<arrow::Schema> derive_schema();
  std::vector<std::shared_ptr<LogicalPlan>> children() override;
  std::string to_string() override;
};
