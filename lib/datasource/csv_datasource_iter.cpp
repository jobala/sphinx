#include <exception>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "csv_datasource_iter.h"
#include "types.h"

template <typename T>
CsvDatasourceIterator<T>::CsvDatasourceIterator(std::ifstream &file, std::shared_ptr<Schema> schema)
    : file_(file), schema_(std::move(schema)){};

template <typename T>
auto CsvDatasourceIterator<T>::next() -> T
{
  std::unordered_map<std::string, std::vector<String>> columns;
  std::vector<ColumnVector> cols;
  for (auto const &field : schema_->fields())
  {
    std::string idx;
    auto status = field->metadata()->Get("idx").Value(&idx);
    columns.insert({idx, {}});
  }

  int read_lines = 0;
  while (read_lines < batch_size_)
  {
    std::string row;
    std::getline(file_, row);
    auto row_items = extract_row_items(row);

    for (int i = 0; i < int(row_items.size()); i++)
    {
      try
      {
        columns.at(std::to_string(i)).push_back(row_items.at(i));
      } catch (const std::exception &e)
      {
        continue;
      }
    }
    read_lines += 1;
  }

  std::vector<ArrowFieldVector> field_vectors;
  for (auto const &field : schema_->fields())
  {
    std::string idx;
    auto status = field->metadata()->Get("idx").Value(&idx);

    auto const &items = columns.at(idx);

    arrow::StringBuilder builder;
    auto appen_status = builder.AppendValues(items);
    std::shared_ptr<arrow::Array> array;
    auto stat = builder.Finish().Value(&array);

    ArrowFieldVector field_vector(array);
    field_vectors.push_back(field_vector);
  }

  return {schema_, field_vectors};
}

template <typename T>
auto CsvDatasourceIterator<T>::extract_row_items(const std::string &row) -> std::vector<String>
{
  std::stringstream sstream(row);
  std::vector<std::string> items;
  std::string item;

  while (std::getline(sstream, item, ','))
  {
    items.push_back(item);
  }

  return items;
}

template <typename T>
auto CsvDatasourceIterator<T>::has_next() -> bool
{
  return file_.eof();
}

template class CsvDatasourceIterator<RecordBatch>;
