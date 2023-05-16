.PHONY: benchmark clean

# Run benchmark test
benchmark:
	@echo "Benchmarking..."
	@if [ -f benchmark/source/ratings_8000000.csv.gz ]; then \
			gzip -d benchmark/source/ratings_8000000.csv.gz; \
	fi
	@cd benchmark && python benchmark.py

# Clean benchmark output files
clean:
	@cd benchmark && find . -name "output*.csv" -type f -delete

	