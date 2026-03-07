#include "logical_plan.h"
#include <memory>
#include <vector>

class Projection : public LogicalPlan
{
  std::shared_ptr<LogicalPlan> input_;
  std::vector<std::shared_ptr<LogicalExpr>> expr_;

public:
  Projection(std::shared_ptr<LogicalPlan> &input, std::vector<std::shared_ptr<LogicalExpr>> &expr);
  std::shared_ptr<arrow::Schema> schema() override;
  std::vector<std::shared_ptr<LogicalPlan>> children() override;
  std::string to_string() override;
};
