#pragma once

#include <arrow/api.h>
#include <vector>

class LogicalPlan
{
public:
  virtual ~LogicalPlan() = default;
  virtual std::shared_ptr<arrow::Schema> schema() = 0;
  virtual std::vector<LogicalPlan &> children() = 0;
};

class LogicalExpr
{
public:
  virtual ~LogicalExpr() = default;
  virtual arrow::Field to_field();
};
