# 定义编译器和编译选项
CC = gcc
CFLAGS = -g -Wall -DDEBUG   # 无需 -c，需要链接
# LDFLAGS = -L../../assignment1/storage_manager/  # 指定静态库所在目录（assignment1 的路径）
LDFLAGS = -L./
LDLIBS = -lstorage_mgr          # 指定链接的静态库（libstorage_mgr.a，省略前缀 lib 和后缀 .a）

# 目标可执行文件
TARGET1 = test_assign2_1
TARGET2 = test_assign2_2

# 同时生成静态库文件
LIB = libbuffer_mgr.a

# 源文件列表（缓冲管理器和测试代码）
SRCS1 = buffer_mgr.c test_assign2_1.c buffer_mgr_stat.c
SRCS2 = buffer_mgr.c test_assign2_2.c buffer_mgr_stat.c
LIB_SRCS = buffer_mgr.c buffer_mgr_stat.c
# 对应的目标文件
OBJS1 = $(SRCS1:.c=.o)
OBJS2 = $(SRCS2:.c=.o)
LIB_OBJS = $(LIB_SRCS:.c=.o)

# 默认目标：生成可执行文件和库文件
all: $(TARGET1) $(TARGET2) $(LIB)

# 链接：将 .o 文件与静态库链接为可执行文件
$(TARGET1): $(OBJS1)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)
	@echo "已生成可执行文件 $@"
$(TARGET2): $(OBJS2)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ $(LDLIBS)
	@echo "已生成可执行文件 $@"
$(LIB): $(LIB_OBJS)
	ar rcs $@ $(LIB_OBJS)
	@echo "已生成静态库 $@"

# 编译 .c 文件为 .o 文件
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# 清理中间文件和可执行文件
clean:
	rm -f $(OBJS1) $(TARGET1) $(OBJS2) $(TARGET2)