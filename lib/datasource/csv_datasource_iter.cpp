#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "csv_datasource_iter.h"
#include "types.h"

template <typename T>
CsvDatasourceIterator<T>::CsvDatasourceIterator(std::ifstream &file, std::shared_ptr<Schema> schema)
    : file_(file), schema_(std::move(schema)){};

template <typename T>
auto CsvDatasourceIterator<T>::next() -> T
{
  /*
   * 1. Read BATCH_SIZE lines or untile end of file
   * 2. Assume all columns are of type string
   * 3. Does getline remember file position?
   *
   * */
  int read_lines = 0;
  while (read_lines < batch_size_)
  {
    std::string row;
    std::getline(file_, row);
    read_lines += 1;
  }

  auto fields = std::vector<std::shared_ptr<ColumnVector>>();
  return {schema_, fields};
}

template <typename T>
auto CsvDatasourceIterator<T>::extract_fields(const std::string &row) -> std::vector<std::shared_ptr<arrow::Field>>
{
  std::stringstream sstream(row);
  std::string field_item;
  std::vector<std::shared_ptr<arrow::Field>> fields;

  while (std::getline(sstream, field_item, ','))
  {
    auto field = arrow::field(field_item, ArrowTypes::StringType);
  }

  return fields;
}

template <typename T>
auto CsvDatasourceIterator<T>::has_next() -> bool
{
  return file_.eof();
}

template class CsvDatasourceIterator<RecordBatch>;
