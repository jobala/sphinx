#include <memory>
#include <vector>

#include "csv_datasource_iter.h"
#include "types.h"

template <typename T>
auto CsvDatasourceIterator<T>::next() -> T
{
  auto fields = std::vector<std::shared_ptr<ColumnVector>>();
  return {schema_, fields};
}

template <typename T>
auto CsvDatasourceIterator<T>::has_next() -> bool
{
  return false;
}

template class CsvDatasourceIterator<RecordBatch>;
