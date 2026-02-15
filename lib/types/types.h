#pragma once

#include <any>
#include <arrow/api.h>
#include <memory>
#include <stdexcept>
#include <vector>

namespace ArrowTypes
{
inline const std::shared_ptr<arrow::DataType> BooleanType = arrow::boolean();

inline const std::shared_ptr<arrow::DataType> Int8Type = arrow::int8();
inline const std::shared_ptr<arrow::DataType> Int16Type = arrow::int16();
inline const std::shared_ptr<arrow::DataType> Int32Type = arrow::int32();
inline const std::shared_ptr<arrow::DataType> Int64Type = arrow::int64();

inline const std::shared_ptr<arrow::DataType> UInt8Type = arrow::uint8();
inline const std::shared_ptr<arrow::DataType> UInt16Type = arrow::uint16();
inline const std::shared_ptr<arrow::DataType> UInt32Type = arrow::uint32();
inline const std::shared_ptr<arrow::DataType> UInt64Type = arrow::uint64();

inline const std::shared_ptr<arrow::DataType> FloatType = arrow::float32();
inline const std::shared_ptr<arrow::DataType> DoubleType = arrow::float64();

inline const std::shared_ptr<arrow::DataType> StringType = arrow::utf8();
} // namespace ArrowTypes

class ColumnVector
{
public:
  virtual ~ColumnVector() = default;
  virtual arrow::Type::type GetType() = 0;
  virtual std::any GetValue(int idx) = 0;
  virtual int Size() = 0;
};

class ArrowFieldVector : public ColumnVector
{
  std::shared_ptr<arrow::Array> field_arr;

public:
  ArrowFieldVector(std::shared_ptr<arrow::Array> &field_arr);

  arrow::Type::type GetType() override;
  std::any GetValue(int idx) override;
  int Size() override;
};

class LiteralValueVector : ColumnVector
{
  arrow::Type::type arrowType;
  std::any value;
  int size;

public:
  arrow::Type::type GetType() override { return arrowType; };
  std::any GetValue(int idx) override
  {
    if (idx < 0 || idx >= size)
    {
      throw std::runtime_error("index out of bounds");
    }
    return value;
  }

  int Size() override { return size; };
};

// todo: add tests for record batch construction
class RecordBatch
{
  std::shared_ptr<arrow::Schema> schema;
  std::vector<std::shared_ptr<ColumnVector>> fields;

public:
  RecordBatch(std::shared_ptr<arrow::Schema> schema, std::vector<std::shared_ptr<ColumnVector>> fields)
  {
    this->schema = std::move(schema);
    this->fields = std::move(fields);
  }

  auto RowCount() -> int { return fields[0]->Size(); }

  auto ColumnCount() -> int { return static_cast<int>(fields.size()); }

  auto GetField(int idx) -> std::shared_ptr<ColumnVector> { return fields[idx]; }
};
