PACKAGE := pack.ag/amqp

.PHONY: fuzz
fuzz:
	go-fuzz-build -o ./go-fuzz/fuzz.zip $(PACKAGE)
	go-fuzz -bin ./go-fuzz/fuzz.zip -workdir go-fuzz

.PHONY: fuzzclean
fuzzclean:
	rm -f ./go-fuzz/{crashers,suppressions}/*

.PHONY: test
test:
	go test -v -race ./...
