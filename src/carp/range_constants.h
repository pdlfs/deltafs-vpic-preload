#pragma once

/*
 * private CARP constants.   using preprocessor rather than const
 * to avoid unused variable warnings.
 */

#define CARP_DEF_PVTCNT      256     /* default pivot count */
#define CARP_DEF_OOBSZ       512     /* default oob buffer size (#particles) */
#define CARP_DEF_RENEGPOLICY "InvocationIntraEpoch"


#define CARP_MAXPIVOTS       8192
#define CARP_MAXPARTSZ       256     /* max particle sz (key+filename+data) */
#define CARP_RENEG_INT       500000
#define CARP_FLOATCOMP_THOLD 1e-3

#if 0
#define DEFAULT_PVTCNT 256
#define DEFAULT_OOBSZ 512
namespace pdlfs {
const size_t kRenegInterval = 500000;
const char* kRenegPolicyIntraEpoch = "InvocationIntraEpoch";
}  // namespace pdlfs
#endif
