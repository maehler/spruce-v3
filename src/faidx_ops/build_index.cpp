#include <iostream>
#include <index.h>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <bz2stream.h>

int main(int argc, char ** argv)
{

  FAidx idx(argv[1], argv[2]);
  idx.from_fasta(argv[2]);
  idx.save();
  return 0;
}
