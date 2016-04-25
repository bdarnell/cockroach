#include <string>
#include "rocksdb/perf_context.h"
#include "profile.h"

void ProfileStart() {
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTime);
  rocksdb::perf_context.Reset();
}
void ProfileStop() {
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  printf("%s\n", rocksdb::perf_context.ToString().c_str());
}
