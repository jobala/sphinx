#include "datasource.h"
#include "types.h"
#include <memory>
#include <vector>

template <typename T>
auto CsvDatasourceIterator<T>::next() -> std::unique_ptr<T>
{
  auto fields = std::vector<std::shared_ptr<ColumnVector>>();
  return std::make_unique<T>(schema_, fields);
}

template <typename T>
auto CsvDatasourceIterator<T>::has_next() -> bool
{
  return false;
}

template class CsvDatasourceIterator<RecordBatch>;
