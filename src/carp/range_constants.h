#pragma once

#define DEFAULT_PVTCNT 256

#define DEFAULT_OOBSZ 512

namespace pdlfs {
static const size_t kMaxPivots = 8192;
static const size_t kMaxPartSize = 256;
static const size_t kRenegInterval = 500000;
static const float kFloatCompThreshold = 1e-3;
static const float kDynamicThreshold = 2.0f;
static const char* kDefaultRenegPolicy = "InvocationPeriodic";
}  // namespace pdlfs
