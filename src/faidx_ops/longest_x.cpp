#include <iostream>
#include <fstream>
#include <string>
#include <index.h>
#include <vector>
#include <fastxio_record.h>
#include <fastxio_reader.h>

void usage(char ** argv)
{
  std::cerr << "Usage: " << argv[0] << " <idx> <fasta> <genome_size> <coverage>\n";
}
  
int main(int argc, char ** argv)
{

  if (argc != 5)
    {
      usage(argv);
      return 22;
    }
  
  FAidx idx(argv[1], argv[2]);
  idx.load();

  uint64_t genome_size = std::stoul(std::string(argv[3]));
  uint64_t target_coverage = std::stoul(std::string(argv[4]));
  uint64_t target_size = genome_size * target_coverage;

  uint64_t tot_len = 0;

  FASTX::Reader R(argv[2], DNA_SEQTYPE);
  
  for (auto it = idx.records().rbegin(); it != idx.records().rend(); it++)
    {
      R.seek(it->offset);
      std::cout << R.next() << '\n';
      tot_len += it->len;
      if (tot_len >= target_size)
	break;
    }
  
  return 0;
}
