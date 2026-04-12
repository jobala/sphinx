#include "projection.h"
#include "logical_plan.h"
#include <format>
#include <memory>
#include <vector>

Projection::Projection(std::shared_ptr<LogicalPlan> input, std::vector<std::shared_ptr<LogicalExpr>> expr)
    : input_(input), expr_(expr)
{
}

auto Projection::schema() -> std::shared_ptr<arrow::Schema>
{
  std::vector<std::shared_ptr<arrow::Field>> fields(0);
  for (auto &exp : expr_)
  {
    fields.push_back(exp->to_field(input_));
  }

  return arrow::schema(fields);
}

auto Projection::children() -> std::vector<std::shared_ptr<LogicalPlan>>
{
  return std::vector<std::shared_ptr<LogicalPlan>>{input_};
}

auto Projection::to_string() -> std::string
{
  std::string columns;

  for (auto &exp : expr_)
  {
    columns.append(exp->to_field(input_)->name() + " ");
  }

  return std::format("Projection: {}", columns);
}
