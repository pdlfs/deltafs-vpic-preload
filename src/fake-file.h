#pragma once
/*
 * Copyright (c) 2016 Carnegie Mellon University.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

/*
 * FakeFile is a replacement for FILE* that we use to accumulate all the
 * VPIC particle data before sending it to the shuffle layer (on fclose).
 * 
 * we assume only one thread is writing to the file at a time, so we
 * do not put a mutex on it.   we ignore out of memory errors.
 */

#include <string.h>
#include <string>

namespace deltafspreload {

class FakeFile {
    std::string path_;         /* path of particle file (malloc'd c++) */
    char data_[32];            /* enough for one VPIC particle */
    char *dptr_;               /* ptr to next free space in data_ */
    int resid_;                /* residual */   

  public:
    explicit FakeFile(const char *path) : 
        path_(path), dptr_(data_), resid_(sizeof(data_)) {};
    
    /* returns actual number of bytes added */
    int AddData(const void *toadd, int len) {
        int n = (len > resid_) ? resid_ : len;
        if (n) {
            memcpy(dptr_, toadd, n);
             dptr_ += n;
             resid_ -= n;
        }
        return(n);
    }

    /* recover filename from FakeFile */
    const char *FileName()  { return path_.c_str(); }

    /* get data */
    char *Data() { return data_; }

    /* get data length */
    int DataLen() { return sizeof(data_) - resid_; }

};


}
