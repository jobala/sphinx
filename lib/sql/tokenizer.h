#pragma once

#include <optional>
#include <string>
#include <vector>

#include "token.h"

class SqlTokenizer
{
  std::string sql_;
  int offset_;

  int skip_whitespace(int start_offset);
  int get_offset_until_terminated_char(unsigned char terminated, int start_offset);
  Token scan_identifier(int start_offset);
  Token scan_symbol(int start_offset);

public:
  SqlTokenizer(const std::string &sql);
  std::vector<Token> tokenize();
  std::optional<Token> next_token();
};
