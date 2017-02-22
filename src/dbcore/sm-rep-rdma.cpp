#include "sm-rep.h"
#include "sm-rep-rdma.h"
#include "../benchmarks/ndb_wrapper.h"

namespace rep {
std::vector<struct RdmaNode*> nodes;
std::mutex nodes_lock;

const static uint32_t kRdmaImmNewSeg = 1U << 31;

void primary_rdma_poll_send_cq(uint64_t nops) {
  ALWAYS_ASSERT(!config::is_backup_srv());
  for(auto& n : nodes) {
    n->PollSendCompletionAsPrimary(nops);
  }
}

void primary_rdma_wait_for_message(uint64_t msg, bool reset) {
  ALWAYS_ASSERT(!config::is_backup_srv());
  for(auto& n : nodes) {
    n->WaitForMessageAsPrimary(kRdmaPersisted | kRdmaReadyToReceive, false);
  }
}

// A daemon that runs on the primary for bringing up backups by shipping
// the latest chkpt (if any) + the log that follows (if any). Uses RDMA
// based memcpy to transfer chkpt and log file data.
void primary_daemon_rdma() {
  // Create an RdmaNode object for each backup node
  for(uint32_t i = 0; i < config::num_backups; ++i) {
    RdmaNode* rn = new RdmaNode(true);
    nodes.push_back(rn);

    int chkpt_fd = -1;
    LSN chkpt_start_lsn = INVALID_LSN;
    auto* md = prepare_start_metadata(chkpt_fd, chkpt_start_lsn);
    ALWAYS_ASSERT(chkpt_fd != -1);
    // Wait for the 'go' signal from the peer
    rn->WaitForMessageAsPrimary(kRdmaReadyToReceive);

    // Now can really send something, metadata first, header must fit in the buffer
    ALWAYS_ASSERT(md->size() < RdmaNode::kDaemonBufferSize);
    char* daemon_buffer = rn->GetDaemonBuffer();
    memcpy(daemon_buffer, md, md->size());

    uint64_t buf_offset = md->size();
    uint64_t to_send = md->chkpt_size;
    while(to_send) {
      ALWAYS_ASSERT(buf_offset <= RdmaNode::kDaemonBufferSize);
      if(buf_offset == RdmaNode::kDaemonBufferSize) {
        // Full, wait for the backup to finish processing this
        rn->WaitForMessageAsPrimary(kRdmaReadyToReceive);
        buf_offset = 0;
      }
      uint64_t n = read(chkpt_fd, daemon_buffer + buf_offset,
                        std::min(to_send, RdmaNode::kDaemonBufferSize - buf_offset));
      to_send -=n;

      // Write it out, then wait
      rn->RdmaWriteImmDaemonBuffer(0, 0, buf_offset + n, buf_offset + n);
      buf_offset = 0;
      rn->WaitForMessageAsPrimary(kRdmaReadyToReceive);
    }
    os_close(chkpt_fd);

    // Done with the chkpt file, now log files
    send_log_files_after_rdma(rn, md, chkpt_start_lsn);
    ++config::num_active_backups;
    __sync_synchronize();
  }
}

void send_log_files_after_rdma(RdmaNode* self, backup_start_metadata* md,
                               LSN chkpt_start) {
  char* daemon_buffer = self->GetDaemonBuffer();
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  for(uint32_t i = 0; i < md->num_log_files; ++i) {
    uint32_t segnum = 0;
    uint64_t start_offset = 0, end_offset = 0; 
    char canary_unused;
    backup_start_metadata::log_segment* ls = md->get_log_segment(i);
    int n = sscanf(ls->file_name.buf, SEGMENT_FILE_NAME_FMT "%c",
                   &segnum, &start_offset, &end_offset, &canary_unused);
    ALWAYS_ASSERT(n == 3);
    uint64_t to_send = ls->size;
    if(to_send) {
      // Ship only the part after chkpt start
      auto* seg = logmgr->get_offset_segment(start_offset);
      int log_fd = os_openat(dfd, ls->file_name.buf, O_RDONLY);
      lseek(log_fd, start_offset - seg->start_offset, SEEK_SET);
      while(to_send) {
        uint64_t n = read(log_fd, daemon_buffer,
                          std::min((uint64_t)RdmaNode::kDaemonBufferSize, to_send));
        ALWAYS_ASSERT(n);
        self->RdmaWriteImmDaemonBuffer(0, 0, n, n);
        to_send -= n;
        self->WaitForMessageAsPrimary(kRdmaReadyToReceive);
      }
      os_close(log_fd);
    }
  }
}

void backup_daemon_rdam(RdmaNode* self) {
  while(!volatile_read(logmgr)) { /** spin **/ }
  rcu_register();
  DEFER(rcu_deregister());
  DEFER(delete self);

  uint32_t size = 0;
  //if (not config::log_ship_sync_redo) {
  //  std::thread rt(redo_daemon);
  //  rt.detach();
  //}

  LOG(INFO) << "[Backup] Start to wait for logs from primary";
  auto* logbuf = sm_log::get_logbuf();
  LSN start_lsn = logmgr->durable_flushed_lsn();
  while (1) {
    rcu_enter();
    DEFER(rcu_exit());
    self->SetMessageAsBackup(kRdmaReadyToReceive);
    // Post an RR to get the log buffer partition bounds
    // Must post RR before setting ReadyToReceive msg (i.e., RR should be posted before
    // the peer sends data).
    auto bounds_array_size = self->ReceiveImm();
    ALWAYS_ASSERT(bounds_array_size == sizeof(uint64_t) * kMaxLogBufferPartitions);

#ifndef NDEBUG
    for(uint32_t i = 0; i < config::logbuf_partitions; ++i) {
      uint64_t s = 0;
      if(i > 0) {
        s = (logbuf_partition_bounds[i] >> 16) - (logbuf_partition_bounds[i-1] >> 16);
      }
      LOG(INFO) << "Logbuf partition: " << i << " " << std::hex
                << logbuf_partition_bounds[i] << std::dec << " " << s;
    }
#endif

    // post an RR to get the data and the chunk's begin LSN embedded as an the wr_id
    uint32_t imm = 0;
    size = self->ReceiveImm(&imm);
    THROW_IF(not size, illegal_argument, "Invalid data size");

    segment_id *sid = logmgr->get_segment(start_lsn.segment());
    if(imm & kRdmaImmNewSeg) {
      // Extract the segment's start offset (off of the segment's raw, theoreticall start offset)
      start_lsn = LSN::make(
        config::log_segment_mb * config::MB * start_lsn.segment() + (imm & ~kRdmaImmNewSeg),
        start_lsn.segment() + 1);
    }
    uint64_t end_lsn_offset = start_lsn.offset() + size;
    sid = logmgr->assign_segment(start_lsn.offset(), end_lsn_offset);

    // now we should already have data sitting in the buffer, but we need
    // to use the data size we got to calculate a new durable lsn first.
    ALWAYS_ASSERT(sid->end_offset >= end_lsn_offset);
    ALWAYS_ASSERT(sid && sid->segnum == start_lsn.segment());
    if(imm & kRdmaImmNewSeg) {
      // Fix the start and end offsets for the new segment
      sid->start_offset = start_lsn.offset();
      sid->end_offset = sid->start_offset + config::log_segment_mb * config::MB;

      // Now we're ready to create the file with correct file name
      logmgr->create_segment_file(sid);
      ALWAYS_ASSERT(sid->start_offset == start_lsn.offset());
      ALWAYS_ASSERT(sid->end_offset >= end_lsn_offset);
    }
    DLOG(INFO) << "Assigned " << std::hex << start_lsn.offset() << "-" << end_lsn_offset
      << " to " << std::dec << sid->segnum << " " << std::hex
      << sid->start_offset << " " << sid->byte_offset << std::dec;
    ALWAYS_ASSERT(sid);
    LSN end_lsn = LSN::make(end_lsn_offset, start_lsn.segment());

    DLOG(INFO) << "[Backup] Received " << size << " bytes ("
      << std::hex << start_lsn.offset() << "-" << end_lsn_offset << std::dec << ")";

    // TODO(tzwang): persist and ack Persisted msg here

    if (config::log_ship_sync_redo) {
      if(logbuf->available_to_read() < size) {
        uint64_t new_byte = sid->byte_offset + (end_lsn_offset - sid->start_offset);
        logbuf->advance_writer(new_byte);
      }
      ALWAYS_ASSERT(logbuf->available_to_read() >= size);
      //logmgr->redo_log(start_lsn, end_lsn);
      logmgr->redo_logbuf(start_lsn, end_lsn);
      printf("[Backup] Rolled forward log %lx-%lx\n", start_lsn.offset(), end_lsn_offset);
    }

    // Now persist the log records in storage
    logmgr->flush_log_buffer(*logbuf, end_lsn_offset, true);
    self->SetMessageAsBackup(kRdmaPersisted);
    ASSERT(logmgr->durable_flushed_lsn().offset() == end_lsn_offset);
    // FIXME(tzwang): now advance cur_lsn so that readers can really use the new data
    
    // Next iteration
    start_lsn = end_lsn;
  }
}

void start_as_backup_rdma() {
  memset(logbuf_partition_bounds, 0, sizeof(uint64_t) * kMaxLogBufferPartitions);
  ALWAYS_ASSERT(config::is_backup_srv());
  RdmaNode* rn = new RdmaNode(false);

  // Tell the primary to start
  rn->SetMessageAsBackup(kRdmaReadyToReceive);

  // Let data come
  uint64_t to_process = rn->ReceiveImm();
  ALWAYS_ASSERT(to_process);

  // Process the header first
  char* buf = rn->GetDaemonBuffer();
  // Get a copy of md to preserve the log file names
  backup_start_metadata* md = (backup_start_metadata*)buf;
  backup_start_metadata* d = (backup_start_metadata*)malloc(md->size());
  memcpy(d, md, md->size());
  md = d;
  md->persist_marker_files();

  to_process -= md->size();
  if(md->chkpt_size > 0) {
    dirent_iterator dir(config::log_dir.c_str());
    int dfd = dir.dup();
    char canary_unused;
    uint64_t chkpt_start = 0, chkpt_end_unused;
    int n = sscanf(md->chkpt_marker, CHKPT_FILE_NAME_FMT "%c",
                   &chkpt_start, &chkpt_end_unused, &canary_unused);
    static char chkpt_fname[CHKPT_DATA_FILE_NAME_BUFSZ];
    n = os_snprintf(chkpt_fname, sizeof(chkpt_fname),
                           CHKPT_DATA_FILE_NAME_FMT, chkpt_start);
    int chkpt_fd = os_openat(dfd, chkpt_fname, O_CREAT|O_WRONLY);
    LOG(INFO) << "[Backup] Checkpoint " << chkpt_fname << ", " << md->chkpt_size << " bytes";

    // Flush out the bytes in the buffer stored after the metadata first
    if(to_process > 0) {
      uint64_t to_write = std::min(md->chkpt_size, to_process);
      os_write(chkpt_fd, buf + md->size(), to_write);
      os_fsync(chkpt_fd);
      to_process -= to_write;
      md->chkpt_size -= to_write;
    }
    ALWAYS_ASSERT(to_process == 0);  // XXX(tzwang): currently always have chkpt

    // More coming in the buffer
    while(md->chkpt_size > 0) {
      // Let the primary know to begin the next round
      rn->SetMessageAsBackup(kRdmaReadyToReceive);
      uint64_t to_write = rn->ReceiveImm();
      os_write(chkpt_fd, buf, std::min(md->chkpt_size, to_write));
      md->chkpt_size -= to_write;
    }
    os_fsync(chkpt_fd);
    os_close(chkpt_fd);
    LOG(INFO) << "[Backup] Received " << chkpt_fname;
  }

  // Now get log files
  dirent_iterator dir(config::log_dir.c_str());
  int dfd = dir.dup();
  for(uint64_t i = 0; i < md->num_log_files; ++i) {
    backup_start_metadata::log_segment* ls = md->get_log_segment(i);
    uint64_t file_size = ls->size;
    int log_fd = os_openat(dfd, ls->file_name.buf, O_CREAT|O_WRONLY);
    ALWAYS_ASSERT(log_fd > 0);
    while(file_size > 0) {
      rn->SetMessageAsBackup(kRdmaReadyToReceive);
      uint64_t received_bytes = rn->ReceiveImm();
      ALWAYS_ASSERT(received_bytes);
      file_size -= received_bytes;
      os_write(log_fd, buf, received_bytes);
    }
    os_fsync(log_fd);
    os_close(log_fd);
  }

  logmgr = sm_log::new_log(config::recover_functor, nullptr);
  sm_oid_mgr::create();

  if(recover_first) {
    ALWAYS_ASSERT(oidmgr);
    logmgr->recover();
  }

  rn->SetMessageAsBackup(kRdmaReadyToReceive);
  LOG(INFO) << "[Backup] Received log file.";

  // Start a daemon to receive and persist future log records
  std::thread t(backup_daemon_rdam, rn);
  t.detach();

  if(!recover_first) {
    // Now we proceed to recovery
    ALWAYS_ASSERT(oidmgr);
    logmgr->recover();
  }
}

// Support only one peer for now
void primary_ship_log_buffer_rdma(const char *buf, uint32_t size,
                                  bool new_seg, uint64_t new_seg_start_offset) {
#ifndef NDEBUG
  for(uint32_t i = 0; i < config::logbuf_partitions; ++i) {
    LOG(INFO) << "RDMA write logbuf bounds: " << i << " " << logbuf_partition_bounds[i];
  }
#endif
  for(auto& node : nodes) {
    node->WaitForMessageAsPrimary(kRdmaReadyToReceive);

    // Send buffer partition boundary information first
    node->RdmaWriteImmLogBufferPartitionBounds(0, 0,
                                     sizeof(uint64_t) * kMaxLogBufferPartitions,
                                     sizeof(uint64_t) * kMaxLogBufferPartitions,
                                     false /* async */);

    ALWAYS_ASSERT(size);
    uint64_t offset = buf - sm_log::get_logbuf()->_data;
    ASSERT(offset + size <= sm_log::get_logbuf()->window_size() * 2);
#ifndef NDEBUG
    if(new_seg) {
      LOG(INFO) << "new segment start offset=" << std::hex << new_seg_start_offset << std::dec;
    }
#endif
    ALWAYS_ASSERT(new_seg_start_offset <= ~kRdmaImmNewSeg);
    uint32_t imm = new_seg ? kRdmaImmNewSeg | (uint32_t)new_seg_start_offset : 0;
    node->RdmaWriteImmLogBuffer(offset, offset, size, imm, false);
  }
}

}  // namespace rep
