#pragma once

#include <fastxio_record.h>
#include <fastxio_reader.h>
#include <bz2stream.h>
#include <memory>
#include <string>
#include <cstdint>
#include <vector>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/utility.hpp>
#include <boost/serialization/list.hpp>
#include <boost/serialization/assume_abstract.hpp>
#include <boost/serialization/vector.hpp>

class index_record {
 public:
  friend class boost::serialization::access;
   template<class Archive>
    void serialize(Archive & ar, const unsigned int version = 1)
    {
      ar & len;
      ar & na;
      ar & ng;
      ar & nc;
      ar & nt;
      ar & nn;
      ar & hash;
      ar & offset;
      ar & id;
    }
   bool operator<(const index_record& rhs)
   {
     return len < rhs.len;
   }
  uint32_t len;
  uint32_t na;
  uint32_t ng;
  uint32_t nc;
  uint32_t nt;
  uint32_t nn;
  uint32_t hash;
  long long offset;
  std::string id;
};

class FAidx
{
 public:
  // Serialization stuff
  friend class boost::serialization::access;
  template<class Archive>
    void serialize(Archive & ar, const unsigned int version = 1)
    {
      ar & BOOST_SERIALIZATION_NVP(_records);
      ar & _fasta;
    }
  void load();
  void save();
  // Ctors
  FAidx(const std::string& index, const std::string& fasta);
  // Create new index
  void from_fasta(const std::string& infile);
  void from_fasta(const std::string& infile, const uint64_t capacity);
  // Data access
  std::vector<index_record>& records() {return _records;}
 private:
  bool _is_read = false;
  bool _is_written = false;
  std::unique_ptr<std::istream> _istream;
  std::unique_ptr<std::ostream> _ostream;
  std::string _fasta;
  std::vector<index_record> _records;
  std::string _index;
};
