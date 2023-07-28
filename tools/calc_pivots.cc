/*
 * calc_pivots  command line tool that calculates and prints pivots
 * 20-Jun-2023  chuck@ece.cmu.edu
 */

/*
 * usage:
 * calc_pivots [bin config] data pivot_count data0 [data1 ...]
 *
 * where bin config format is: b0[:cnt0] b1[:cnt1] ... bn
 */
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <carp/oob_buffer.h>
#include <carp/pivots.h>

int main(int argc, char **argv) {
  int lcv, nbins, pcloc, pivot_count;
  char *endptr;
  if (argc < 3) {
    printf("usage: %s [bin config] data pivot_count data0 [data1 ...]\n",
           *argv);
    printf("  where bin config format is: b0[:cnt0] b1[:cnt1] ... bn\n");
    exit(1);
  }

  /* get bin size */
  for (nbins = 0, lcv = 1 ; lcv < argc ; lcv++) {
    if (strcmp(argv[lcv], "data") == 0)
      break;
    nbins++;
  }
  if (lcv >= argc) {
    fprintf(stderr, "err: no 'data' string on command line\n");
    exit(1);
  }
  if (nbins == 1) {
    fprintf(stderr, "err: number of bin config points must be 0 or >1\n");
    exit(1);
  } else if (nbins > 1) {
    nbins--;     /* adjust for ending entry */
  }

  float bin_array[nbins+1];
  uint64_t wt_array[nbins];
  if (nbins) {
    for (lcv = 1 ; lcv < nbins+2 ; lcv++) {
      float b = strtof(argv[lcv], &endptr);
      if (*endptr && *endptr != ':') {
        printf("bin parse error: %s\n", argv[lcv]);
        exit(1);
      }
      if (lcv > 1 && b < bin_array[lcv-2]) {
        printf("bin sort error at %d (%f < %f)\n", lcv, b, bin_array[lcv-2]);
        exit(1);
      }
      uint64_t w = (*endptr == ':') ? atoi(endptr+1) : 0;
      bin_array[lcv-1] = b;
      if (lcv-1 < nbins) {
        wt_array[lcv-1] = w;
      }
    }
  }

  /*
   * nbins can't be 0, set to 1 if we are not using them.
   * the bins will remain in 'unset' state unless we load arrays.
   */
  pdlfs::carp::OrderedBins bins((nbins < 1) ? 1 : nbins);

  if (nbins) {
    bins.UpdateFromArrays(nbins, bin_array, wt_array);  /* bins set */
  }

  pcloc = (nbins) ? nbins + 3 : 2;  /* one past 'data' */
  if (pcloc >= argc) {
    printf("err: missing pivot count\n");
    exit(1);
  }
  pivot_count = atoi(argv[pcloc]);
  if (pivot_count < 2) {
    printf("err: bad pivot count %s ... must be >= 2\n", argv[pcloc]);
    exit(1);
  }

  /* set oob max size large enough to handle remaining args */
  pdlfs::carp::OobBuffer oob(argc - pcloc + 1);

  if (bins.IsSet()) {
    oob.SetInBoundsRange(bins.GetRange());
  }

  if (pcloc + 1 >= argc) {
    printf("err: no data on command line to load\n");
    exit(1);
  }

  /* load data in */
  for (lcv = pcloc + 1 ; lcv < argc ; lcv++) {
    float prop = strtof(argv[lcv], &endptr);
    if (*endptr) {
      printf("parse error: %s\n", argv[lcv]);
      exit(1);
    }
    if (oob.OutOfBounds(prop)) {
      pdlfs::carp::particle_mem_t p;
      p.indexed_prop = prop;
      p.buf_sz = 0;
      p.shuffle_dest = -1;
      if (oob.Insert(p) != 0) {
        printf("insert oob failed!\n");
        exit(1);
      }
      printf("insert oob: %f\n", prop);
    } else {
      size_t bidx;
      int rv = bins.SearchBins(prop, bidx, false);
      if (rv != 0) {
        printf("search bin failed!\n");
        exit(1);
      }
      bins.IncrementBin(bidx);
      printf("insert bins: %f (bidx=%zd)\n", prop, bidx);
    }
  }

  printf("load complete\n");
  printf("  total bin weight = %" PRIu64 "\n", bins.GetTotalWeight());
  printf("  total oob weight = %zd\n\n", oob.Size());

  pdlfs::carp::ComboConsumer<float,uint64_t> cco(&bins, &oob);
  pdlfs::carp::Pivots piv(pivot_count);

  piv.Calculate(cco);

  printf("result pivots: size=%zd, weight=%lf\n", piv.Size(),
         piv.PivotWeight());
  for (size_t i = 0 ; i < piv.Size() ; i++) {
    double p = piv[i];
    printf("piv[%zd] = %lf%s\n", i, p, (trunc(p) != p) ? " (frac)" : "");
  }
  
  exit(0);
}
