#include "types.h"
#include <any>
#include <memory>

ArrowFieldVector::ArrowFieldVector(std::shared_ptr<arrow::Array> &field_arr) : field_arr(field_arr) {}

auto ArrowFieldVector::GetType() -> arrow::Type::type { return field_arr->type_id(); }

auto ArrowFieldVector::Size() -> int { return static_cast<int>(field_arr->length()); }

auto ArrowFieldVector::GetValue(int idx) -> std::any
{
  switch (field_arr->type_id())
  {
  case arrow::Type::BOOL:
  {
    auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(field_arr);
    return bool_array->Value(idx);
  }
  case arrow::Type::INT8:
  {
    auto int_array = std::static_pointer_cast<arrow::Int8Array>(field_arr);
    return int_array->Value(idx);
  }
  case arrow::Type::INT16:
  {
    auto int_array = std::static_pointer_cast<arrow::Int16Array>(field_arr);
    return int_array->Value(idx);
  }
  case arrow::Type::INT32:
  {
    auto int_array = std::static_pointer_cast<arrow::Int32Array>(field_arr);
    return int_array->Value(idx);
  }
  case arrow::Type::INT64:
  {
    auto int_array = std::static_pointer_cast<arrow::Int64Array>(field_arr);
    return int_array->Value(idx);
  }
  case arrow::Type::STRING:
  {
    auto string_array = std::static_pointer_cast<arrow::StringArray>(field_arr);
    return std::string(string_array->GetView(idx));
  }
  case arrow::Type::DOUBLE:
  {
    auto double_array = std::static_pointer_cast<arrow::DoubleArray>(field_arr);
    return double_array->Value(idx);
  }
  default:
    throw std::runtime_error("Unsupported vector type");
  }
}
