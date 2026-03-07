#include "logical_plan.h"
#include <memory>
class Selection : public LogicalPlan
{
  std::shared_ptr<LogicalPlan> input_;
  std::shared_ptr<LogicalExpr> expr_;

public:
  Selection(std::shared_ptr<LogicalPlan> &input, std::shared_ptr<LogicalExpr> &expr);
  std::shared_ptr<arrow::Schema> schema() override;
  std::vector<std::shared_ptr<LogicalPlan>> children() override;
  std::string to_string() override;
};
