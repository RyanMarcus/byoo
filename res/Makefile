BASEFILES=$(wildcard inputs/imdb/*.csv.xz)
CSV_TARGETS=$(subst .csv.xz,.csv,$(BASEFILES))
BYOO_TARGETS=$(subst .csv.xz,.byoo,$(BASEFILES))

all: $(CSV_TARGETS) $(BYOO_TARGETS)

clean:
	rm -f $(CSV_TARGETS) $(BYOO_TARGETS)

inputs/imdb/%.csv: inputs/imdb/%.csv.xz
	unxz -kf $<

inputs/imdb/%.byoo: inputs/imdb/%.csv plans/%.json ../target/release/byoo
	 ../target/release/byoo $(patsubst inputs/imdb/%.byoo,plans/%.json,$@)

../target/release/byoo:
	cargo build --release

.phony: all clean
