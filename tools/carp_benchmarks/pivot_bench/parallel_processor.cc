//
// Created by Ankush J on 10/28/22.
//

#include "parallel_processor.h"

void rank_task_dual(void* args) {
  RankTask* task = (RankTask*)args;
  int* rc = task->jobs_rem;

  if (task->pivots) {
    task->rank->GetPerfectPivots(task->epoch, task->pivots, task->num_pivots);
  } else if (task->bins) {
    task->rank->ReadEpochIntoBins(task->epoch, task->bins);
  }

  task->mutex->Lock();
  (*rc)--;
  if (*rc == 0) {
    task->cv->SignalAll();
  }
  task->mutex->Unlock();
}