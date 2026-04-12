#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "token.h"
#include "tokenizer.h"

TEST(Tokenizer, tokenize_sql_string)
{
  std::string query = "select * from users";

  SqlTokenizer tokenizer(query);
  auto tokens = tokenizer.tokenize();

  std::vector<std::string> res{"select", "*", "from", "users"};
  std::vector<TokenType> token_types{TokenType::SELECT, TokenType::STAR, TokenType::FROM, TokenType::IDENTIFIER};

  ASSERT_EQ(4, tokens.size());
  for (int i = 0; i < (int)tokens.size(); i++)
  {
    ASSERT_EQ(res[i], tokens[i].text_);
    ASSERT_EQ(token_types[i], tokens[i].type_);
  }
}

TEST(Tokenizer, tokenize_projected_sql_string)
{
  std::string query = "select name, age from users";

  SqlTokenizer tokenizer(query);
  auto tokens = tokenizer.tokenize();

  std::vector<std::string> res{"select", "name", ",", "age", "from", "users"};
  std::vector<TokenType> token_types{TokenType::SELECT,     TokenType::IDENTIFIER, TokenType::COMMA,
                                     TokenType::IDENTIFIER, TokenType::FROM,       TokenType::IDENTIFIER};

  ASSERT_EQ(6, tokens.size());
  for (int i = 0; i < (int)tokens.size(); i++)
  {
    ASSERT_EQ(res[i], tokens[i].text_);
    ASSERT_EQ(token_types[i], tokens[i].type_);
  }
}
