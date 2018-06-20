/*
  Author: Bastian Schiffthaler (bastian.schiffhtaler@umu.se)
  Spruce V3: This program will stream FASTA files into a priority queue
  keeping the longest N reads. Needed to filter those out without exploding memory.
 */

#include <iostream>
#include <fastxio_record.h>
#include <fastxio_reader.h>
#include <queue>
#include <string>

std::string usage(""
		  "Usage: longest_n <n> <fasta>"
		  "");

class rev_length_order
{
public:
  bool operator()(const FASTX::Record& lhs, const FASTX::Record& rhs) {
    return lhs.size() > rhs.size();
  }
};

int main(const int argc, const char ** argv)
{
  
  if (argc != 3)
    {
      std::cout << usage << '\n';
      return 22;
    }

  std::priority_queue<FASTX::Record,
		      std::vector<FASTX::Record>,
		      rev_length_order> queue;

  unsigned long n = std::stoul(std::string(argv[1]));
  
  FASTX::Reader R(argv[2], DNA_SEQTYPE);

  while(R.peek() != EOF)
    {
      FASTX::Record r = R.next();
      if (queue.size() < n)
	queue.push(r);
      else if (r.size() > queue.top().size())
	{
	  queue.pop();
	  queue.push(r);
	}
    }

  while (! queue.empty())
    {
      std::cout << queue.top() << '\n';
      queue.pop();
    }
  
  return 0;
}
