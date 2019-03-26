#include <iostream>
#include <index.h>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <bz2stream.h>
#include <string>

std::string usage(""
		  "build_index <idx_file> <fasta> [capacity]\n"
		  "'capacity' is the number of records you want to reserve beforehand"
		  " to prevent the vector from inefficiently growing\n"
		  "");

int main(int argc, char ** argv)
{

  if (argc != 3 && argc != 4)
    {
      std::cerr << usage << '\n';
      return 22;
    }

  
  FAidx idx(argv[1], argv[2]);

  if (argc == 3)
    {
      idx.from_fasta(argv[2]);
    }
  else
    {
      idx.from_fasta(argv[2], std::stoul(argv[3]));
    }
  idx.save();
  return 0;
}
