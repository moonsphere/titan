#include "test_util/sync_point.h"

#include <limits>

#include "util/random.h"

#include "blob_file_iterator.h"
#include "blob_file_reader.h"
#include "blob_file_size_collector.h"
#include "blob_gc_job.h"
#include "blob_gc_picker.h"
#include "db/version_set.h"
#include "db_impl.h"
#include "punch_hole_gc_job.h"
#include "titan_logging.h"
#include "util.h"

namespace rocksdb {
namespace titandb {

Status TitanDBImpl::ExtractGCStatsFromTableProperty(
    const std::shared_ptr<const TableProperties>& table_properties, bool to_add,
    std::map<uint64_t, int64_t>* blob_file_size_diff) {
  assert(blob_file_size_diff != nullptr);
  if (table_properties == nullptr) {
    // No table property found. File may not contain blob indices.
    return Status::OK();
  }
  return ExtractGCStatsFromTableProperty(*table_properties.get(), to_add,
                                         blob_file_size_diff);
}

Status TitanDBImpl::ExtractGCStatsFromTableProperty(
    const TableProperties& table_properties, bool to_add,
    std::map<uint64_t, int64_t>* blob_file_size_diff) {
  assert(blob_file_size_diff != nullptr);
  auto& prop = table_properties.user_collected_properties;
  auto prop_iter = prop.find(BlobFileSizeCollector::kPropertiesName);
  if (prop_iter == prop.end()) {
    // No table property found. File may not contain blob indices.
    return Status::OK();
  }
  Slice prop_slice(prop_iter->second);
  std::map<uint64_t, uint64_t> blob_file_sizes;
  if (!BlobFileSizeCollector::Decode(&prop_slice, &blob_file_sizes)) {
    return Status::Corruption("Failed to decode blob file size property.");
  }
  for (const auto& blob_file_size : blob_file_sizes) {
    uint64_t file_number = blob_file_size.first;
    int64_t diff = static_cast<int64_t>(blob_file_size.second);
    if (!to_add) {
      diff = -diff;
    }
    (*blob_file_size_diff)[file_number] += diff;
  }
  return Status::OK();
}

Status TitanDBImpl::AsyncInitializeGC(
    const std::vector<ColumnFamilyHandle*>& cf_handles) {
  std::vector<std::pair<uint32_t, Version*>> cfs;
  FlushOptions flush_opts;
  flush_opts.wait = true;
  for (ColumnFamilyHandle* cf_handle : cf_handles) {
    std::shared_ptr<BlobStorage> blob_storage =
        blob_file_set_->GetBlobStorage(cf_handle->GetID()).lock();
    assert(blob_storage != nullptr);
    blob_storage->StartInitializeAllFiles();

    // Flush memtable to make sure keys written by GC are all in SSTs.
    auto s = Flush(flush_opts, cf_handle);
    if (!s.ok()) {
      return s;
    }

    // Hold the version and increment the ref count
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(cf_handle);
    auto cfd = cfh->cfd();
    db_impl_->mutex()->Lock();
    auto version = cfd->current();
    version->Ref();
    db_impl_->mutex()->Unlock();

    cfs.push_back(std::make_pair(cf_handle->GetID(), version));
  }

  thread_initialize_gc_.reset(new port::Thread([=]() {
    Status s;

    auto unref = [=](Version* version) {
      db_impl_->mutex()->Lock();
      version->Unref();
      db_impl_->mutex()->Unlock();
    };

    TEST_SYNC_POINT_CALLBACK("TitanDBImpl::AsyncInitializeGC:Begin", this);
    for (auto cf : cfs) {
      if (shuting_down_.load(std::memory_order_acquire)) {
        unref(cf.second);
        continue;
      }
      auto cf_handle = db_impl_->GetColumnFamilyHandleUnlocked(cf.first);
      if (!cf_handle) {
        unref(cf.second);
        continue;
      }
      TITAN_LOG_INFO(db_options_.info_log,
                     "Titan begin async GC initialization on cf [%s]",
                     cf_handle->GetName().c_str());
      TablePropertiesCollection collection;
      // this operation may be slow
      s = cf.second->GetPropertiesOfAllTables(ReadOptions(), &collection);
      unref(cf.second);
      if (!s.ok()) {
        MutexLock l(&mutex_);
        this->SetBGError(s);
        return;
      }

      std::map<uint64_t, int64_t> blob_file_size_diff;
      for (auto& file : collection) {
        s = ExtractGCStatsFromTableProperty(file.second, true /*to_add*/,
                                            &blob_file_size_diff);
        if (!s.ok()) {
          MutexLock l(&mutex_);
          this->SetBGError(s);
          return;
        }
      }

      mutex_.Lock();
      std::shared_ptr<BlobStorage> blob_storage =
          blob_file_set_->GetBlobStorage(cf_handle->GetID()).lock();
      mutex_.Unlock();
      assert(blob_storage != nullptr);
      for (auto& file_size : blob_file_size_diff) {
        assert(file_size.second >= 0);
        std::shared_ptr<BlobFileMeta> file =
            blob_storage->FindFile(file_size.first).lock();
        if (file != nullptr) {
          file->UpdateLiveDataSize(file_size.second);
        }
      }
      blob_storage->InitPunchHoleGCOnStart();
      blob_storage->InitializeAllFiles();
      TITAN_LOG_INFO(db_options_.info_log,
                     "Titan finish async GC initialization on cf [%s]",
                     cf_handle->GetName().c_str());
    }

    if (!shuting_down_.load(std::memory_order_acquire)) {
      TEST_SYNC_POINT_CALLBACK(
          "TitanDBImpl::AsyncInitializeGC:BeforeSetInitialized", this);
      // Initialization done.
      initialized_.store(true, std::memory_order_release);
      {
        MutexLock l(&mutex_);
        for (auto cf : cfs) {
          std::shared_ptr<BlobStorage> blob_storage =
              blob_file_set_->GetBlobStorage(cf.first).lock();
          blob_storage->ComputeGCScore();
          AddToGCQueue(cf.first);
        }
        MaybeScheduleGC();
      }
    }
    TEST_SYNC_POINT_CALLBACK("TitanDBImpl::AsyncInitializeGC:End", this);
  }));

  return Status::OK();
}

void TitanDBImpl::MaybeScheduleGC() {
  mutex_.AssertHeld();

  if (db_options_.disable_background_gc) return;

  if (!initialized_.load(std::memory_order_acquire)) return;

  if (shuting_down_.load(std::memory_order_acquire)) return;

  while (unscheduled_gc_ > 0 &&
         bg_gc_scheduled_ < db_options_.max_background_gc) {
    unscheduled_gc_--;
    bg_gc_scheduled_++;
    thread_pool_->SubmitJob(std::bind(&TitanDBImpl::BGWorkGC, this));
  }
}

void TitanDBImpl::BGWorkGC(void* db) {
  reinterpret_cast<TitanDBImpl*>(db)->BackgroundCallGC();
}

void TitanDBImpl::BackgroundCallGC() {
  TEST_SYNC_POINT("TitanDBImpl::BackgroundCallGC:BeforeGCRunning");
  {
    MutexLock l(&mutex_);
    assert(bg_gc_scheduled_ > 0);
    while (drop_cf_requests_ > 0) {
      bg_cv_.Wait();
    }
    bg_gc_running_++;

    // Try running pending (waiting for the snapshot to be the oldest) punch
    // hole GC first.
    if (!MaybeRunPendingPunchHoleGC()) {
      TEST_SYNC_POINT("TitanDBImpl::BackgroundCallGC:BeforeBackgroundGC");
      if (!gc_queue_.empty()) {
        uint32_t column_family_id = PopFirstFromGCQueue();
        LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                             db_options_.info_log.get());
        BackgroundGC(&log_buffer, column_family_id);
        {
          mutex_.Unlock();
          log_buffer.FlushBufferToLog();
          LogFlush(db_options_.info_log.get());
          mutex_.Lock();
        }
      }
    }
    TEST_SYNC_POINT("TitanDBImpl::BackgroundCallGC:AfterGCRunning");

    bg_gc_running_--;
    bg_gc_scheduled_--;
    MaybeScheduleGC();
    if (bg_gc_scheduled_ == 0 || bg_gc_running_ == 0) {
      // Signal DB destructor if bg_gc_scheduled_ drop to 0.
      // Signal drop CF requests if bg_gc_running_ drop to 0.
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it.
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be deallocated and referencing them
    // will cause trouble.
  }
}

bool TitanDBImpl::MaybeRunPendingPunchHoleGC() {
  mutex_.AssertHeld();
  if (pending_punch_hole_gc_ == nullptr || punch_hole_gc_running_) {
    return false;
  }
  if (blob_file_set_->IsColumnFamilyObsolete(pending_punch_hole_gc_->cf_id())) {
    TITAN_LOG_INFO(db_options_.info_log, "GC skip dropped colum family [%s].",
                   cf_info_[pending_punch_hole_gc_->cf_id()].name.c_str());
    pending_punch_hole_gc_ = nullptr;
    return false;
  }
  if (pending_punch_hole_gc_->snapshot()->GetSequenceNumber() ==
      GetOldestSnapshotSequence()) {
    TITAN_LOG_DEBUG(db_options_.info_log,
                    "Titan start scheduled punch hole GC");
    punch_hole_gc_running_ = true;
    mutex_.Unlock();
    Status s = pending_punch_hole_gc_->Run();
    mutex_.Lock();
    if (s.ok()) {
      TITAN_LOG_DEBUG(db_options_.info_log,
                      "Titan finish scheduled punch hole GC");
      pending_punch_hole_gc_->Finish();
    } else {
      SetBGError(s);
      TITAN_LOG_ERROR(db_options_.info_log,
                      "Titan scheduled punch hole GC error: %s",
                      s.ToString().c_str());
    }
    punch_hole_gc_running_ = false;
    bool need_schedule_gc = pending_punch_hole_gc_->blob_gc()->trigger_next();
    pending_punch_hole_gc_ = nullptr;
    TEST_SYNC_POINT(
        "TitanDBImpl::MaybeRunPendingPunchHoleGC:"
        "AfterRunPendingPunchHoleGC");
    if (need_schedule_gc) {
      MaybeScheduleGC();
    }
    return true;
  }
  return false;
}

Status TitanDBImpl::BackgroundGC(LogBuffer* log_buffer,
                                 uint32_t column_family_id) {
  mutex_.AssertHeld();

  std::unique_ptr<BlobGC> blob_gc;
  std::unique_ptr<ColumnFamilyHandle> cfh;

  std::shared_ptr<BlobStorage> blob_storage;
  // Skip CFs that have been dropped.
  if (!blob_file_set_->IsColumnFamilyObsolete(column_family_id)) {
    blob_storage = blob_file_set_->GetBlobStorage(column_family_id).lock();
  } else {
    TEST_SYNC_POINT_CALLBACK("TitanDBImpl::BackgroundGC:CFDropped", nullptr);
    TITAN_LOG_BUFFER(log_buffer, "GC skip dropped colum family [%s].",
                     cf_info_[column_family_id].name.c_str());
  }
  if (blob_storage != nullptr) {
    const auto& cf_options = blob_storage->cf_options();
    std::shared_ptr<BlobGCPicker> blob_gc_picker =
        std::make_shared<BasicBlobGCPicker>(db_options_, cf_options,
                                            stats_.get());
    const bool allow_punch_hole = cf_options.punch_hole_threshold > 0 &&
                                  pending_punch_hole_gc_ == nullptr;
    blob_gc =
        blob_gc_picker->PickBlobGC(blob_storage.get(), allow_punch_hole);

    // Second chance via the sampling probe: when the tracked ledger sees
    // nothing worth GC, probe a few silent files against the LSM and re-pick
    // with the refreshed estimates. The tracked ratio only grows when
    // compactions drop the referencing keys, which a tiny value-separated
    // SST layer may never trigger.
    if (!blob_gc && cf_options.enable_gc_sampling) {
      std::vector<std::shared_ptr<BlobFileMeta>> to_probe;
      uint64_t now_micros = env_->NowMicros();
      uint64_t min_interval_micros =
          cf_options.gc_sampling_min_interval_seconds * 1000000ULL;
      std::map<uint64_t, std::weak_ptr<BlobFileMeta>> all_files;
      blob_storage->ExportBlobFiles(all_files);
      for (auto& number_and_file : all_files) {
        if (to_probe.size() >= cf_options.gc_sampling_files_per_round) break;
        auto file = number_and_file.second.lock();
        if (file == nullptr ||
            file->file_state() != BlobFileMeta::FileState::kNormal ||
            file->file_size() <= cf_options.merge_small_file_threshold ||
            file->GetDiscardableRatio() >=
                cf_options.blob_file_discardable_ratio ||
            now_micros - file->last_sample_micros() < min_interval_micros) {
          continue;
        }
        to_probe.emplace_back(std::move(file));
      }
      if (!to_probe.empty()) {
        cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);
        assert(column_family_id == cfh->GetID());
        mutex_.Unlock();
        SampleBlobFilesForGC(to_probe, cfh.get(), cf_options);
        mutex_.Lock();
        blob_storage->ComputeGCScore();
        blob_gc =
            blob_gc_picker->PickBlobGC(blob_storage.get(), allow_punch_hole);
      }
    }

    if (blob_gc) {
      if (!cfh) {
        cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);
        assert(column_family_id == cfh->GetID());
      }
      blob_gc->SetColumnFamily(cfh.get());
    }
  }

  Status s;
  // TODO(@DorianZheng) Make sure enough room for GC
  if (UNLIKELY(blob_gc == nullptr)) {
    RecordTick(statistics(stats_.get()), TITAN_GC_NO_NEED, 1);
    // Nothing to do
    TITAN_LOG_BUFFER(log_buffer, "Titan GC nothing to do");
    TEST_SYNC_POINT("TitanDBImpl::BackgroundGC:NothingToDo");
    return s;
  }
  if (blob_gc->punch_hole_gc()) {
    auto snapshot = db_impl_->GetSnapshot();
    pending_punch_hole_gc_ = std::unique_ptr<PunchHoleGCJob>(new PunchHoleGCJob(
        column_family_id, std::move(blob_gc), db_impl_, db_options_, env_,
        env_options_, snapshot, &shuting_down_));
    if (!MaybeRunPendingPunchHoleGC()) {
      MaybeScheduleGC();
    } else {
      TEST_SYNC_POINT("TitanDBImpl::BackgroundGC:RunPunchHoleGCRightAway");
    }
  } else {
    cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);
    assert(column_family_id == cfh->GetID());
    blob_gc->SetColumnFamily(cfh.get());

    StopWatch gc_sw(env_->GetSystemClock().get(), statistics(stats_.get()),
                    TITAN_GC_MICROS);
    BlobGCJob blob_gc_job(blob_gc.get(), db_, &mutex_, db_options_, env_,
                          env_options_, blob_manager_.get(),
                          blob_file_set_.get(), log_buffer, &shuting_down_,
                          stats_.get());
    s = blob_gc_job.Prepare();
    if (s.ok()) {
      mutex_.Unlock();
      // Tag this thread's IO so the embedder can prioritize blob GC IO. The
      // mutex is released here, so any throttling sleep won't block foreground
      // Titan operations.
      if (db_options_.gc_io_hook_enter != nullptr) {
        db_options_.gc_io_hook_enter(db_options_.gc_io_hook_arg);
      }
      TEST_SYNC_POINT("TitanDBImpl::BackgroundGC::BeforeRunGCJob");
      s = blob_gc_job.Run();
      TEST_SYNC_POINT("TitanDBImpl::BackgroundGC::AfterRunGCJob");
      if (db_options_.gc_io_hook_exit != nullptr) {
        db_options_.gc_io_hook_exit(db_options_.gc_io_hook_arg);
      }
      mutex_.Lock();
    }
    if (s.ok()) {
      s = blob_gc_job.Finish();
    }
    blob_gc->ReleaseGcFiles();

    if (blob_gc->trigger_next() &&
        (bg_gc_scheduled_ - 1 + gc_queue_.size() <
         2 * static_cast<uint32_t>(db_options_.max_background_gc))) {
      RecordTick(statistics(stats_.get()), TITAN_GC_TRIGGER_NEXT, 1);
      // There is still data remained to be GCed
      // and the queue is not overwhelmed
      // then put this cf to GC queue for next GC
      AddToGCQueue(blob_gc->column_family_handle()->GetID());
    }

    if (s.ok()) {
      RecordTick(statistics(stats_.get()), TITAN_GC_SUCCESS, 1);
      // Done
    } else {
      SetBGError(s);
      RecordTick(statistics(stats_.get()), TITAN_GC_FAILURE, 1);
      TITAN_LOG_WARN(db_options_.info_log, "Titan GC error: %s",
                     s.ToString().c_str());
    }
  }

  TEST_SYNC_POINT("TitanDBImpl::BackgroundGC:Finish");
  return s;
}

Status TitanDBImpl::TEST_StartGC(uint32_t column_family_id) {
  // BackgroundCallGC
  Status s;
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, db_options_.info_log.get());
  {
    MutexLock l(&mutex_);
    // Prevent CF being dropped while GC is running.
    while (drop_cf_requests_ > 0) {
      bg_cv_.Wait();
    }
    bg_gc_running_++;
    bg_gc_scheduled_++;

    s = BackgroundGC(&log_buffer, column_family_id);

    {
      mutex_.Unlock();
      log_buffer.FlushBufferToLog();
      LogFlush(db_options_.info_log.get());
      mutex_.Lock();
    }

    bg_gc_running_--;
    bg_gc_scheduled_--;
    if (bg_gc_scheduled_ == 0 || bg_gc_running_ == 0) {
      bg_cv_.SignalAll();
    }
  }
  return s;
}

void TitanDBImpl::SampleBlobFilesForGC(
    const std::vector<std::shared_ptr<BlobFileMeta>>& files,
    ColumnFamilyHandle* cfh, const TitanCFOptions& cf_options) {
  // Route probe reads through the dedicated GC rate limiter, same as GC
  // itself, so probing never contends with foreground IO.
  EnvOptions gc_env_options(env_options_);
  if (db_options_.gc_rate_limiter != nullptr) {
    gc_env_options.rate_limiter = db_options_.gc_rate_limiter.get();
  }
  uint64_t records_per_file =
      std::max<uint64_t>(cf_options.gc_sampling_records_per_file, 1);
  for (auto& file : files) {
    if (shuting_down_.load(std::memory_order_acquire)) return;
    file->set_last_sample_micros(env_->NowMicros());
    std::unique_ptr<RandomAccessFileReader> reader;
    Status s = NewBlobFileReader(file->file_number(), 0, db_options_,
                                 gc_env_options, env_, &reader);
    if (!s.ok()) {
      TITAN_LOG_WARN(db_options_.info_log,
                     "Titan GC sampling: open blob file %" PRIu64
                     " failed: %s",
                     file->file_number(), s.ToString().c_str());
      continue;
    }
    BlobFileIterator iter(std::move(reader), file->file_number(),
                          file->file_size(), cf_options);
    // Land before a random record, then walk a contiguous window. The
    // window start is uniform over the file, so with in-place-update
    // workloads (garbage spread evenly) the estimate is unbiased.
    uint64_t entries = file->file_entries();
    if (entries > records_per_file) {
      uint64_t skip = Random::GetTLSInstance()->Uniform(
          static_cast<int>(std::min<uint64_t>(
              entries - records_per_file,
              std::numeric_limits<int32_t>::max())));
      uint64_t approx_offset =
          file->file_size() * skip / std::max<uint64_t>(entries, 1);
      iter.IterateForPrev(approx_offset);
      if (!iter.status().ok()) {
        // Fall back to the file head.
        iter.IterateForPrev(0);
      }
    } else {
      iter.IterateForPrev(0);
    }
    uint64_t sampled = 0;
    uint64_t dead = 0;
    for (iter.Next(); iter.Valid() && sampled < records_per_file;
         iter.Next()) {
      if (shuting_down_.load(std::memory_order_acquire)) return;
      PinnableSlice index_entry;
      bool is_blob_index = false;
      DBImpl::GetImplOptions gopts;
      gopts.column_family = cfh;
      gopts.value = &index_entry;
      gopts.is_blob_index = &is_blob_index;
      Status gs = db_impl_->GetImpl(ReadOptions(), iter.key(), gopts);
      bool discardable = false;
      if (gs.IsNotFound() || (gs.ok() && !is_blob_index)) {
        // Key deleted, or updated with a value now inlined in the LSM.
        discardable = true;
      } else if (gs.ok()) {
        BlobIndex other_blob_index;
        if (!other_blob_index.DecodeFrom(&index_entry).ok()) {
          continue;
        }
        discardable = !(iter.GetBlobIndex() == other_blob_index);
      } else {
        // Read error: skip this record without counting it.
        continue;
      }
      sampled++;
      if (discardable) dead++;
    }
    if (sampled > 0) {
      double ratio = static_cast<double>(dead) / sampled;
      file->set_sampled_discardable_ratio(ratio);
      TITAN_LOG_INFO(db_options_.info_log,
                     "Titan GC sampling: blob file %" PRIu64
                     " sampled %" PRIu64 " records, estimated garbage ratio "
                     "%.2f (tracked-and-sampled max %.2f)",
                     file->file_number(), sampled, ratio,
                     file->GetDiscardableRatio());
    }
  }
}

void TitanDBImpl::TEST_WaitForBackgroundGC() {
  MutexLock l(&mutex_);
  while (bg_gc_scheduled_ > 0) {
    bg_cv_.Wait();
  }
}

}  // namespace titandb
}  // namespace rocksdb
