#include <fastxio_record.h>
#include <fastxio_reader.h>
#include <index.h>
#include <bz2stream.h>
#include <memory>
#include <string>
#include <cstdint>
#include <vector>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/list.hpp>
#include <boost/serialization/assume_abstract.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include <MurmurHash3.h>
#include <algorithm>

FAidx::FAidx(const std::string& index, const std::string& fasta) :
  _fasta(fasta), _index(index)
{}

void FAidx::load()
{
  if (_is_read)
    throw std::runtime_error("Double read of archive");
  _istream.reset(new ibz2stream(_index.c_str()));
  boost::archive::binary_iarchive ia(*_istream);
  ia >> (*this);
}

void FAidx::save()
{
  if (_is_written)
    throw std::runtime_error("Double write of archive");

  std::sort(_records.begin(), _records.end());
  
  _ostream.reset(new obz2stream(_index.c_str()));
  boost::archive::binary_oarchive oa(*_ostream);
  oa << (*this);
}

void FAidx::from_fasta(const std::string& infile)
{
  from_fasta(infile, 1);
}

void FAidx::from_fasta(const std::string& infile, const uint64_t capacity)
{
  _records.reserve(capacity);
  FASTX::Reader R(infile.c_str(), DNA_SEQTYPE);
  while (R.peek() != EOF)
    {
      index_record rec;
      rec.offset = R.tell();
      FASTX::Record r = R.next();

      FASTX::NucFrequency nf;
      nf.add(r);

      rec.len = r.size();

      rec.id = r.get_id();
      
      rec.na = nf['A'];
      rec.nc = nf['C'];
      rec.ng = nf['G'];
      rec.nt = nf['T'];
      rec.nn = nf['N'];

      std::string seq = r.get_seq(); //find more efficient way
      
      MurmurHash3_x86_32(&seq[0], rec.len, 314159265, &rec.hash);
      _records.push_back(rec);
    }
}

