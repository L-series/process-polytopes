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

.PHONY: all clean
all: $(BUILD_DIR)/cws.x

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

-include $(OBJECTS:.o=.d)
