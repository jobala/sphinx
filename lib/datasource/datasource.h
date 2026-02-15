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
  virtual std::unique_ptr<T> next() = 0;
};

using DatasourceIterator = std::unique_ptr<Iterator<RecordBatch>>;

class Datasource
{
public:
  virtual ~Datasource() = default;
  virtual std::shared_ptr<Schema> schema() = 0;
  virtual DatasourceIterator scan(const std::vector<String> &project) = 0;
};

template <typename T>
class CsvDatasourceIterator : public Iterator<T>
{
  int position_ = 0;
  int batch_size_ = 100;
  std::shared_ptr<std::ifstream> file_;
  std::shared_ptr<Schema> schema_;

public:
  CsvDatasourceIterator(std::shared_ptr<std::ifstream> &file, std::shared_ptr<Schema> schema)
      : file_(file), schema_(std::move(schema)) {};

  bool has_next() override;
  std::unique_ptr<T> next() override;
  int operator*() const { return this->position_; }

  CsvDatasourceIterator<T> &operator++()
  {
    ++position_;
    return *this;
  }

  bool operator!=(const CsvDatasourceIterator<T> &other) const { return position_ != other.position_; }
};

class CsvDatasource : public Datasource
{
  std::optional<std::shared_ptr<Schema>> schema_;
  std::shared_ptr<Schema> final_schema_;
  std::shared_ptr<std::ifstream> file_;
  std::shared_ptr<Schema> infer_schema();

public:
  CsvDatasource(std::optional<std::shared_ptr<Schema>> schema, const String &file_name) : schema_(std::move(schema))
  {
    file_ = std::make_shared<std::ifstream>(file_name);
    final_schema_ = schema_.value_or(this->infer_schema());
  };

  std::shared_ptr<Schema> schema() override;
  DatasourceIterator scan(const std::vector<String> &project) override;
};
