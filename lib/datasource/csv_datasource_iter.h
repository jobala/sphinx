#pragma once
#include <arrow/api.h>
#include <fstream>
#include <memory>
#include <string>

#define BATCH_SIZE 100;
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

template <typename T>
class CsvDatasourceIterator : public Iterator<T>
{
  int position_ = 0;
  std::ifstream *file_;
  std::shared_ptr<Schema> schema_;

public:
  CsvDatasourceIterator(std::ifstream *file, std::shared_ptr<Schema> schema)
      : file_(file), schema_(std::move(schema)) {};

  bool has_next() override;
  T next() override;
  int operator*() const { return this->position_; }

  CsvDatasourceIterator<T> &operator++()
  {
    ++position_;
    return *this;
  }

  bool operator!=(const CsvDatasourceIterator<T> &other) const { return position_ != other.position_; }
};
