#pragma once
#include <arrow/api.h>
#include <vector>

#include "datasource.h"
#include "logical_plan.h"

class Scan : public LogicalPlan
{
  std::string path_;
  Datasource &datasource_;
  std::vector<std::string> projection_;

public:
  Scan(std::string path, Datasource &datasource, std::vector<std::string> projection);
  std::shared_ptr<arrow::Schema> schema() override;
  std::vector<LogicalPlan &> children() override;
};
