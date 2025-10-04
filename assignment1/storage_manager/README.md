# These code is CS525 assignment 1
Finished by Wei Wang (A20518191) 
2025-9-1 
## Description
- Request to implement a simple storage manager that supports reading and writing disk files of fixed size (defined by PAGE_SIZE in dberror. h) pages, and provides core operations such as file creation, opening, closing, and destruction; 
- Need to maintain key information for opening files (total page count, current page position, file name, POSIX file descriptor/FILE pointer, etc., managed through the SM_SileHandle structure), implement specified interfaces (including file operations, block read/write methods, and return RC type status codes defined by dberror. h); 
- The code needs to be organized according to the specified directory structure (the assign1 folder contains source files, Makefiles, etc.), and a README.txt file should be submitted to explain the solution and code structure.
## Files
📦 \\wsl.localhost\Ubuntu-22.04\home\sean\cs525\assign1\storage_manager/
    ├─ 📄 test_assign1_1.c # test case file
    ├─ 📄 dberror.h        # error code define
    ├─ 📄 README.md        # This file
    ├─ 📄 test_helper.h    # header file of test case file
    ├─ 📄 storage_mgr.c # storage manager implement
    ├─ 📄 storage_mgr.h    # header file of storage manager
    ├─ 📄 makefile         # make file for compiler
    └─ 📄 dberror.c        # show error code
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
test_assign1
```
## running video 
Please access follow website to find the recoder of code running
https://drive.google.com/file/d/1jtZts9YX5kcwRaNRlolxPahLAe78Gwbr/view?usp=sharing
