# keydb

high performance key value database written in Go

still under construction...

uses LSM trees, see https://en.wikipedia.org/wiki/Log-structured_merge-tree

limitation of max 1024 byte keys, to allow efficient on disk index searching

TODOs : read 'keys' header key into memory for efficient binary search
