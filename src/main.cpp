#include <iostream>
#include <planner.h>

auto main() -> int {
  Greeter greeter;

  greeter.say_hello("Japheth");
  greeter.say_hello("World");

  std::cout << "Hello! " << "\n";
  std::cout << "Hello, world!";
}
