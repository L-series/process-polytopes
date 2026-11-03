# Project front end; PALP's own standalone build remains in PALP/.
CC ?= cc
CFLAGS ?= -O3 -g -Wall -Wextra
BUILD_DIR ?= build
POLY_DMAX ?= 5
CPPFLAGS += -DPOLY_Dmax=$(POLY_DMAX) -IPALP -Isrc/cws
PALP_SOURCES = Coord Rat Vertex Polynf LG cws
PROJECT_SOURCES = main catalogue weight_pool
OBJECTS = $(addprefix $(BUILD_DIR)/palp-,$(addsuffix .o,$(PALP_SOURCES))) \
          $(addprefix $(BUILD_DIR)/project-,$(addsuffix .o,$(PROJECT_SOURCES)))

.PHONY: all clean gpu
all: $(BUILD_DIR)/cws.x

# CUDA front end (src/gpu). Not part of `all`: it needs nvcc. Set CUDA_ARCH for
# the target GPU (sm_120 RTX PRO 6000 Blackwell, sm_89 L40 / RTX 6000 Ada).
NVCC ?= nvcc
CUDA_ARCH ?= sm_120
NVCCFLAGS ?= -O3 -std=c++17 -lineinfo
GPU_OBJECTS = $(BUILD_DIR)/project-catalogue.o $(BUILD_DIR)/gpu-catalogue_bridge.o
gpu: $(BUILD_DIR)/cws-gpu.x

$(BUILD_DIR)/cws-gpu.x: src/gpu/cws_gpu.cu $(wildcard src/gpu/*.cuh) src/gpu/catalogue_bridge.h $(GPU_OBJECTS)
	$(NVCC) $(NVCCFLAGS) -gencode arch=compute_$(CUDA_ARCH:sm_%=%),code=$(CUDA_ARCH) -Isrc/gpu \
	  -o $@ src/gpu/cws_gpu.cu $(GPU_OBJECTS)

$(BUILD_DIR)/gpu-%.o: src/gpu/%.c $(BUILD_DIR)/config | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) -Isrc/gpu $(CFLAGS) -MMD -MP -c $< -o $@

$(BUILD_DIR)/cws.x: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^ -lm

$(BUILD_DIR)/palp-%.o: PALP/%.c $(BUILD_DIR)/config | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) -MMD -MP -c $< -o $@

$(BUILD_DIR)/palp-cws.o: CPPFLAGS += -DPALP_CWS_NO_MAIN

$(BUILD_DIR)/project-%.o: src/cws/%.c $(BUILD_DIR)/config | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) -MMD -MP -c $< -o $@

$(BUILD_DIR):
	mkdir -p $@

# Only change the timestamp when compile settings change, so objects built
# with different dimension-dependent layouts can never be mixed silently.
.PHONY: FORCE
FORCE:

$(BUILD_DIR)/config: FORCE | $(BUILD_DIR)
	@printf '%s\n' '$(CC)' '$(CPPFLAGS)' '$(CFLAGS)' > $@.tmp
	@cmp -s $@.tmp $@ && rm $@.tmp || mv $@.tmp $@

clean:
	rm -f $(OBJECTS) $(OBJECTS:.o=.d) $(BUILD_DIR)/cws.x $(BUILD_DIR)/config
	rm -f $(GPU_OBJECTS) $(GPU_OBJECTS:.o=.d) $(BUILD_DIR)/cws-gpu.x

-include $(OBJECTS:.o=.d) $(GPU_OBJECTS:.o=.d)
