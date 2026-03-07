#pragma once
#include <memory>

#include "../logical_plan.h"
#define COLUMN_NOT_FOUND "column not found"

class Column : public LogicalExpr
{
  std::string name_;

public:
  Column(std::string &name);
  std::shared_ptr<arrow::Field> to_field(std::shared_ptr<LogicalPlan> &input) override;
  std::string to_string() override;
};
