# 定义编译器和编译选项
CC = gcc
CFLAGS = -g -Wall -DDEBUG   # 无需 -c，需要链接
# LDFLAGS = -L../../assignment1/storage_manager/  # 指定静态库所在目录（assignment1 的路径）
LDFLAGS = -L./
LDLIBS = -lbuffer_mgr -lstorage_mgr         # 指定链接的静态库（libbuffer_mgr.a，省略前缀 lib 和后缀 .a）

# 目标可执行文件
TARGET1 = test_assign3_1
TARGET2 = test_expr

# 同时生成静态库文件
LIB = librecord_mgr.a

# 源文件列表（记录管理器和测试代码）
SRCS1 = record_mgr.c dberror.c rm_serializer.c expr.c test_assign3_1.c
SRCS2 = record_mgr.c dberror.c rm_serializer.c expr.c test_expr.c
LIB_SRCS = record_mgr.c dberror.c rm_serializer.c expr.c
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