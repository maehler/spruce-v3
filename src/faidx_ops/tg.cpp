#include <iostream>
#include <fstream>
#include <string>
#include <fastxio_reader.h>

int main(int argc, char ** argv)
{

  /*
  std::ifstream ifs(argv[1]);

  std::string line;
  std::cout << ifs.tellg() << '\n';
  while (std::getline(ifs, line))
    {
      std::getline(ifs, line);
      std::cout << (int)ifs.tellg() << '\n';
      }*/

  FASTX::Reader R(argv[1], DNA_SEQTYPE);

  while (R.peek() != EOF)
    {
      auto x = R.tell();
      std::cout << typeid(x).name() << '\n';
      std::cout << R.tell() << '\n';
      R.next();
    }
  
  return 0;
}
