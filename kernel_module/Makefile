obj-m += autogmem_kmod.o

LDFLAGS=-L/usr/src/nvidia-545.23.08

KERNEL_DIR := /lib/modules/$(shell uname -r)/build
ROOT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
SRC_DIR := $(ROOT_DIR)
BUILD_DIR := $(ROOT_DIR)/build
BUILD_DIR_MAKEFILE := $(BUILD_DIR)/Makefile

all: $(BUILD_DIR_MAKEFILE)
	make -C $(KERNEL_DIR) LD_FLAGS=$(LDFLAGS) M=$(BUILD_DIR) src=$(SRC_DIR) modules

$(BUILD_DIR):
	mkdir -p "$@"


$(BUILD_DIR_MAKEFILE): $(BUILD_DIR)
	touch "$@"

clean:
	make -C $(KERNEL_DIR) M=$(BUILD_DIR) src=$(SRC_DIR) clean
	rm -rf $(BUILD_DIR)