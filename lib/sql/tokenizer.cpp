#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "tokenizer.h"

SqlTokenizer::SqlTokenizer(const std::string &sql) : sql_(sql) {}

auto SqlTokenizer::tokenize() -> std::vector<Token>
{
  std::vector<Token> res;

  auto token = next_token();
  while (token.has_value())
  {
    res.push_back(token.value());
    token = next_token();
  }

  return res;
}

auto SqlTokenizer::next_token() -> std::optional<Token>
{
  auto offset = skip_whitespace(offset_);
  if (offset > (int)sql_.length())
  {
    return std::nullopt;
  }

  if (Literal::is_identifier_start(sql_[offset]))
  {
    auto token = scan_identifier(offset);
    offset = token.end_offset_;
    return token;
  }

  if (Literal::is_number_start(sql_[offset]))
  {
    throw std::runtime_error("Not Implemented");
  }

  if (Literal::is_char_start(sql_[offset]))
  {
    throw std::runtime_error("Not Implemented");
  }

  return std::nullopt;
}
auto SqlTokenizer::skip_whitespace(int start_offset) -> int
{
  auto end_offset = start_offset;
  while (end_offset < (int)sql_.size() && sql_[end_offset] == ' ')
  {
    end_offset += 1;
  }
  return end_offset;
}

auto SqlTokenizer::scan_identifier(int start_offset) -> Token
{
  if (offset_ < (int)sql_.size() && '`' == sql_[offset_])
  {
    auto end_offset = get_offset_until_terminated_char('`', start_offset);
    auto text = sql_.substr(start_offset, end_offset);
    return {text, TokenType::IDENTIFIER, end_offset + 1};
  }

  auto end_offset = start_offset;
  while (end_offset < (int)sql_.size() && Literal::is_identifier_part(sql_[end_offset]))
  {
    end_offset += 1;
  }

  auto text = sql_.substr(start_offset, end_offset);
  auto token_type = Type::from_string(text);
  return {text, token_type, end_offset + 1};
}

auto SqlTokenizer::get_offset_until_terminated_char(unsigned char terminated, int start_offset) -> int
{
  auto end_offset = start_offset;
  while (end_offset < (int)sql_.size() && static_cast<unsigned char>(sql_[end_offset]) != terminated)
  {
    end_offset += 1;
  }
  return end_offset;
}
