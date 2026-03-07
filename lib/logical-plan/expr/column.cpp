#include "column.h"
#include <memory>
#include <stdexcept>
#include <string>

Column::Column(std::string &name) : name_(name) {}

auto Column::to_field(std::shared_ptr<LogicalPlan> &input) -> std::shared_ptr<arrow::Field>
{
  std::optional<std::shared_ptr<arrow::Field>> field;

  for (const auto &schema_field : input->schema()->fields())
  {
    if (schema_field->name() == name_)
    {
      field = schema_field;
      break;
    }
  }

  if (!field.has_value())
  {
    throw std::runtime_error(COLUMN_NOT_FOUND);
  }

  return field.value();
}

auto Column::to_string() -> std::string { return name_; }
