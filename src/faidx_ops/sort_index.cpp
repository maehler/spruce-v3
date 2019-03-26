#include <iostream>
#include <index.h>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <string>

std::string usage(""
		  "build_index <idx_file> <fasta>\n"
		  );

int main(int argc, char ** argv)
{

  if (argc != 3)
    {
      std::cerr << usage << '\n';
      return 22;
    }

  FAidx idx(argv[1], argv[2]);
  std::cerr << "Loading index...\n";
  idx.load();
  std::cerr << "Sorting index...\n";
  idx.sort();
  std::cerr << "Saving index...\n";
  idx.save();

  return 0;
}
