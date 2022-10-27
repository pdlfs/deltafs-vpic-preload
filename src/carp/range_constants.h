#pragma once

#define DEFAULT_PVTCNT 256

#define DEFAULT_OOBSZ 512

namespace pdlfs {
static const size_t kMaxPivots = 64; // TODO: changed for MacOS, tmp
static const size_t kMaxPartSize = 256;
static const size_t kRenegInterval = 500000;
static const float kFloatCompThreshold = 1e-3;
static const char* kRenegPolicyIntraEpoch = "InvocationIntraEpoch";
static const char* kRenegPolicyInterEpoch = "InvocationInterEpoch";
static const char* kDefaultRenegPolicy = kRenegPolicyIntraEpoch;
}  // namespace pdlfs
