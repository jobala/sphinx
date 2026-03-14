#pragma once

#include <cctype>
#include <string>
#include <unordered_map>

enum class TokenType : std::uint8_t {
  // keywords
  SELECT,
  FROM,

  // literals
  LONG,
  DOUBLE,
  STRING,
  IDENTIFIER
};

namespace Type
{
inline TokenType from_string(std::string &token)
{
  // uppercase token
  for (auto &tkn : token)
  {
    toupper(tkn);
  }

  static const std::unordered_map<std::string, TokenType> keywords = {{"SELECT", TokenType::SELECT},
                                                                      {"FROM", TokenType::FROM}};

  auto iter = keywords.find(token);
  return iter != keywords.end() ? iter->second : TokenType::IDENTIFIER;
}

} // namespace Type
struct Literal
{
  static bool is_number_start(unsigned char letter) { return std::isdigit(letter) != 0 || letter == '.'; }
  static bool is_identifier_start(char letter) { return std::isalpha(letter) != 0 || letter == '`'; }
  static bool is_char_start(char letter) { return letter == '\'' || '"' == letter; }

  static bool is_identifier_part(char letter)
  {
    return std::isdigit(letter) != 0 || std::isalpha(letter) != 0 || letter == '_';
  }
};

struct Token
{
  std::string text_;
  TokenType type_;
  int end_offset_;

  Token(std::string &text, TokenType type, int end_offset) : text_(text), type_(type), end_offset_(end_offset) {}
};
