**LD_PRELOAD library for deltafs VPIC demo.**

[![CI](https://github.com/pdlfs/deltafs-vpic-preload/actions/workflows/ci.yml/badge.svg)](https://github.com/pdlfs/deltafs-vpic-preload/actions/workflows/ci.yml)
[![GitHub release](https://img.shields.io/github/release/pdlfs/deltafs-vpic-preload.svg)](https://github.com/pdlfs/deltafs-vpic-preload/releases)
[![License](https://img.shields.io/badge/license-New%20BSD-blue.svg)](LICENSE.txt)

DeltaFS VPIC Preload
====================

```
XXXXXXXXX
XX      XX                 XX                  XXXXXXXXXXX
XX       XX                XX                  XX
XX        XX               XX                  XX
XX         XX              XX   XX             XX
XX          XX             XX   XX             XXXXXXXXX
XX           XX  XXXXXXX   XX XXXXXXXXXXXXXXX  XX         XX
XX          XX  XX     XX  XX   XX       XX XX XX      XX
XX         XX  XX       XX XX   XX      XX  XX XX    XX
XX        XX   XXXXXXXXXX  XX   XX     XX   XX XX    XXXXXXXX
XX       XX    XX          XX   XX    XX    XX XX           XX
XX      XX      XX      XX XX   XX X    XX  XX XX         XX
XXXXXXXXX        XXXXXXX   XX    XX        XX  XX      XX
```

DeltaFS was developed, in part, under U.S. Government contract 89233218CNA000001 for Los Alamos National Laboratory (LANL), which is operated by Triad National Security, LLC for the U.S. Department of Energy/National Nuclear Security Administration. Please see the accompanying [LICENSE.txt](LICENSE.txt) for further information.

Range TODO
==========

1. You can receive RENEG\_BEGIN from the next round while you're still processing pivot updates from the current round.
2. You can receive RENEG\_PIVOTS from current round before you have seen RENEG\_BEGIN from the current round. This can be solved using an implicit Phase 1 message.
