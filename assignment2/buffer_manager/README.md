# These code is CS525 assignment 2
Finished by Wei Wang (A20518191) 
2025-9-17 
## Description
This code implements a buffer manager that manages a buffer pool for page-based storage. It supports multiple page replacement strategies including FIFO, LRU, CLOCK, LFU, and LRU-K. Core functionalities include initializing/shutting down the buffer pool, pinning/unpinning pages, marking pages as dirty, flushing dirty pages to disk, and tracking read/write I/O operations. The implementation also includes test cases to verify the correctness of buffer pool operations, replacement strategies, and error handling scenarios.
## Files
📦 CS525\assignment2\buffer_manager/
    ├─ 📄 buffer_mgr.h // header file of buffer manager
    ├─ 📄 dberror.h    // error code define
    ├─ 📄 README.md    // this file
    ├─ 📄 buffer_mgr_stat.c // buffer manager statistic implement
    ├─ 📄 test_helper.h  // header file of test case file
    ├─ 📄 storage_mgr.h  // header file of storage manager
    ├─ 📄 dt.h           // some basic define  
    ├─ 📄 test_assign2_2.c // test case file 2 
    ├─ 📄 test_assign2_1.c // test case file 1
    ├─ 📄 makefile         // make file for compiler
    ├─ 📄 buffer_mgr_stat.h  // header file of buffer manager statistic
    ├─ 📄 libstorage_mgr.a  // library file of storage manager
    └─ 📄 buffer_mgr.c       // buffer manager implement
## Environment Setting based on Windows
- Install wsl on PC
    I use Ubuntu 22.04 LTS.
- Update system
``` bash
sudo apt update && sudo apt upgrade -y
```
- Install compiler, debug tools and git
``` bash
sudo apt install -y gcc gdb make git
```
## Create make file
Create a basic make file for compiler

## compile and test
``` bash
make
test_assign2_1
test_assign2_2
```
## running video 
Please access follow website to find the recoder of code running
https://drive.google.com/file/d/1cli3O31bIJhglkMQMYdQ_A7hk-QPptfq/view?usp=sharing
