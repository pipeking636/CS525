# 定义编译器和编译选项
CC = gcc
CFLAGS = -g -Wall -DDEBUG

# 目标可执行文件和静态库
TARGET = test_assign1
LIB = libstorage_mgr.a

# 源文件划分
TEST_SRCS = test_assign1_1.c       # 仅测试相关代码
LIB_SRCS = dberror.c storage_mgr.c # 库相关代码（不含测试）
ALL_SRCS = $(TEST_SRCS) $(LIB_SRCS)

# 目标文件
TEST_OBJS = $(TEST_SRCS:.c=.o)
LIB_OBJS = $(LIB_SRCS:.c=.o)
ALL_OBJS = $(ALL_SRCS:.c=.o)

# 默认目标：优先构建库，再构建可执行文件
all: $(LIB) $(TARGET)
#	@echo "编译完成：可执行文件=$(TARGET)，静态库=$(LIB)"

# 构建可执行文件（显式依赖静态库）
$(TARGET): $(TEST_OBJS) $(LIB)
	$(CC) $(CFLAGS) -o $@ $(TEST_OBJS) $(LIB)
#	@echo "已生成可执行文件 $@"

# 构建静态库
$(LIB): $(LIB_OBJS)
	ar rcs $@ $(LIB_OBJS)
#	@echo "已生成静态库 $@"

# 编译规则
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# 清理
clean:
	rm -f $(ALL_OBJS) $(TARGET) $(LIB)
#	@echo "已清理所有生成文件"

# 伪目标声明
.PHONY: all clean
    