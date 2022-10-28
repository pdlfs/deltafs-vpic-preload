//
// Created by Ankush J on 10/28/22.
//

#include "parallel_processor.h"

static void read_rank_into_oob_pivots(void* args) {
  PivotTask* task = (PivotTask*)args;
  int* rc = task->jobs_rem;

  logf(LOG_INFO, "Getting pivots for rank %d\n", task->rank);

  pdlfs::carp::PivotCalcCtx pvt_ctx;
  task->tr->ReadRankIntoPivotCtx(task->epoch, task->rank, &pvt_ctx,
                                 task->oobsz);
  pdlfs::carp::PivotUtils::CalculatePivots(&pvt_ctx, task->pivots,
                                           task->num_pivots);
  task->mutex->Lock();
  (*rc)--;
  if (*rc == 0) {
    task->cv->SignalAll();
  }
  task->mutex->Unlock();
}

static void read_rank_into_bins(void* args) {
  PivotTask* task = (PivotTask*)args;
  int* rc = task->jobs_rem;

  logf(LOG_INFO, "Getting bins for rank %d\n", task->rank);

  pdlfs::carp::PivotCalcCtx pvt_ctx;
  task->tr->ReadRankIntoBins(task->epoch, task->rank, *task->bins);

  task->mutex->Lock();
  (*rc)--;
  if (*rc == 0) {
    task->cv->SignalAll();
  }
  task->mutex->Unlock();
}
