#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "types.h"

using Schema = arrow::Schema;
using String = std::string;

template <typename T>
class Iterator
{
public:
  virtual ~Iterator() = default;
  virtual bool has_next() = 0;
  virtual T next() = 0;
};

class Datasource
{
public:
  virtual ~Datasource() = default;
  virtual arrow::Schema schema() = 0;
  virtual Iterator<RecordBatch> Scan(std::vector<String> project) = 0;
};

class CsvDatasource : public Datasource
{
  std::optional<std::shared_ptr<Schema>> schema_;
  std::shared_ptr<Schema> final_schema_;
  std::ifstream file_;
  std::shared_ptr<Schema> infer_schema();

public:
  CsvDatasource(std::optional<std::shared_ptr<Schema>> schema, const String &file_name)
      : schema_(std::move(schema)), file_(file_name)
  {
    final_schema_ = schema_.value_or(this->infer_schema());
  };

  Schema schema() override;
  Iterator<RecordBatch> Scan(std::vector<String> project) override;
};

template <typename T>
class CsvDatasourceIterator : public Iterator<T>
{
  int position_ = 0;
  int batch_size_ = 100;
  std::ifstream file_;
  std::shared_ptr<Schema> schema_;

public:
  CsvDatasourceIterator(std::ifstream &file, std::shared_ptr<Schema> schema)
      : file_(std::move(file)), schema_(std::move(schema)) {};

  bool has_next() override;
  T next() override;
  T operator*() { return this->position_; }

  CsvDatasourceIterator<T> &operator++()
  {
    ++position_;
    return *this;
  }

  bool operator!=(const CsvDatasourceIterator<T> &other) const { return position_ != other.position_; }
};
