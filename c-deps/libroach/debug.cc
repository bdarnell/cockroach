// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

#include <libroach.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <util/file_reader_writer.h>
#include <db/log_reader.h>
#include "chunked_buffer.h"
#include "status.h"

using namespace cockroach;

namespace {

struct ErrReporter : public rocksdb::log::Reader::Reporter {
  virtual void Corruption(size_t bytes, const rocksdb::Status& s) override {
    std::cerr << "Corruption detected in log file " << s.ToString() << "\n" << bytes << " bytes in buffer\n";
    status = s;
    saved_buf = *scratch;
  }

  std::string* scratch;
  rocksdb::Status status;
  std::string saved_buf;
};

}  // unnamed namespace

DBStatus DBReadWALFile(DBSlice filename, DBString* data) {
  auto env = rocksdb::Env::Default();
  std::unique_ptr<rocksdb::SequentialFile> seq_file;
  rocksdb::Status status = env->NewSequentialFile(
      std::string(filename.data, filename.len),
      &seq_file, rocksdb::EnvOptions());
  if (!status.ok()) {
    return ToDBStatus(status);
  }
  std::unique_ptr<rocksdb::SequentialFileReader> seq_file_reader(
      new rocksdb::SequentialFileReader(std::move(seq_file)));
  rocksdb::DBOptions db_options;
  std::string scratch;
  ErrReporter reporter;
  reporter.scratch = &scratch;
  rocksdb::log::Reader log_reader(db_options.info_log, std::move(seq_file_reader),
      &reporter, true, 0, 0);

  rocksdb::Slice record;
  // Our output format is a batch containing a sequence of puts, each
  // of which contains a log entry (itself a WriteBatch) as its value.
  rocksdb::WriteBatch batch;
  while (log_reader.ReadRecord(&record, &scratch, rocksdb::WALRecoveryMode::kAbsoluteConsistency)) {
    std::string s = "hi";
    status = batch.Put(s, record);
    if (!status.ok()) {
      return ToDBStatus(status);
    }
  }

  if (!reporter.status.ok()) {
    status = batch.Put("hi", reporter.saved_buf);
    if (!status.ok()) {
      return ToDBStatus(status);
    }
  }

  std::string repr = batch.Data();
  *data = ToDBString(repr);

  return kSuccess;
}
