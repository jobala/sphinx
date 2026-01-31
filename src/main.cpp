#include <iostream>
#include <planner.h>

int main() {
  Greeter greeter;

  greeter.say_hello("Japheth");
  greeter.say_hello("World");

  std::cout << "Hello! " << std::endl;
  std::cout << "Hello, world!";
}
