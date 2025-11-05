# 定义编译器和编译选项
CC = gcc
CFLAGS = -g -Wall   # 无需 -c，需要链接

# 目标可执行文件
TARGET1 = test_btree
TARGET3 = test_btree3

# 源文件列表（记录管理器和测试代码）
SRCS1 = test_btree.c
# SRCS3 = test_btree3.c

# 对应的目标文件
OBJS1 = $(SRCS1:.c=.o)
# OBJS3 = $(SRCS3:.c=.o)

# 默认目标：生成可执行文件和库文件
all: $(TARGET1)

# 链接：将 .o 文件链接为可执行文件
$(TARGET1): $(OBJS1)
	$(CC) $(CFLAGS) $(OBJS1) -o $(TARGET1) -lm
	@echo "已生成可执行文件 $@"
# $(TARGET3): $(OBJS3)
# 	$(CC) $(CFLAGS) $(OBJS3) -o $(TARGET3) -lm
# 	@echo "已生成可执行文件 $@"
# 编译 .c 文件为 .o 文件
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# 清理中间文件和可执行文件
clean:
	rm -f $(OBJS1) $(TARGET1)
	@echo "已清理中间文件和可执行文件"
# 	rm -f $(OBJS3) $(TARGET3)
# 	@echo "已清理中间文件和可执行文件"