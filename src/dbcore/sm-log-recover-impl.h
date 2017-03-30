#pragma once

#include "../benchmarks/ordered_index.h"
#include "sm-config.h"
#include "sm-thread.h"
#include "sm-log-recover.h"

/* The base functor class that implements common methods needed
 * by most recovery methods. The specific recovery method can
 * inherit this guy and implement its own way of recovery, e.g.,
 * parallel replay by file/OID partition, etc.
 */
struct sm_log_recover_impl {
  void recover_insert(sm_log_scan_mgr::record_scan *logrec, bool latest = false);
  void recover_index_insert(sm_log_scan_mgr::record_scan *logrec);
  void recover_update(sm_log_scan_mgr::record_scan *logrec, bool is_delete, bool latest);
  void recover_update_key(sm_log_scan_mgr::record_scan* logrec);
  fat_ptr recover_prepare_version(
                              sm_log_scan_mgr::record_scan *logrec,
                              fat_ptr next);
  OrderedIndex *recover_fid(sm_log_scan_mgr::record_scan *logrec);
  void recover_index_insert(
      sm_log_scan_mgr::record_scan *logrec, OrderedIndex* index);

  // The main recovery function; the inheriting class should implement this
  // The implementation shall replay the log from position [from] until [to],
  // no more and no less; this is important for async log replay on backups.
  // Recovery at startup however can give [from]=chkpt_begin, and [to]=+inf
  // to replay the whole log.
  virtual void operator()(void *arg, sm_log_scan_mgr *scanner, LSN from, LSN to) = 0;
};

struct parallel_oid_replay : public sm_log_recover_impl {
  struct redo_runner : public thread::sm_runner {
    parallel_oid_replay *owner;
    OID oid_partition;
    bool done;

    redo_runner(parallel_oid_replay *o, OID part) :
      thread::sm_runner(), owner(o), oid_partition(part), done(false) {}
    virtual void my_work(char *);
    void redo_partition();
  };

  uint32_t nredoers;
  std::vector<struct redo_runner> redoers;
  sm_log_scan_mgr *scanner;
  LSN start_lsn;
  LSN end_lsn;

  parallel_oid_replay() : nredoers(10) {} //config::worker_threads) {}
  virtual void operator()(void *arg, sm_log_scan_mgr *scanner, LSN from, LSN to);
};

// A special case that each thread will replay a given range of LSN offsets
// that are guaranteed to respect log block/transaction boundaries. Used by
// replay during log shipping.
struct parallel_offset_replay : public sm_log_recover_impl {
  struct redo_runner : public thread::sm_runner {
    parallel_offset_replay *owner;
    // The half-open interval
    LSN start_lsn;
    LSN end_lsn;
    bool done;

    redo_runner(parallel_offset_replay *o, LSN start, LSN end) :
      thread::sm_runner(), owner(o), start_lsn(start), end_lsn(end) {}
    virtual void my_work(char *);
    void redo_logbuf_partition();
  };

  uint32_t nredoers;
  std::vector<struct redo_runner> redoers;
  sm_log_scan_mgr *scanner;

  parallel_offset_replay() : nredoers(config::num_backup_replay_threads()) {}
  virtual void operator()(void *arg, sm_log_scan_mgr *scanner, LSN from, LSN to);
};

