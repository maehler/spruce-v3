#include <iostream>
#include <index.h>

int main(int argc, char ** argv)
{

  FAidx idx(argv[1], argv[2]);
  idx.load();

  std::cout << "ID\tLength\tA\tC\tG\tT\tN\tOffset\tHash\n";
  for (auto& i : idx.records())
    {
      std::cout
	<< i.id << '\t'
	<< i.len << '\t'
	<< i.na << '\t'
	<< i.nc << '\t'
	<< i.ng << '\t'
	<< i.nt << '\t'
	<< i.nn << '\t'
	<< i.offset << '\t'
	<< i.hash << '\n';
    }
  
  return 0;
}
