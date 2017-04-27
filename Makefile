PACKAGE := pack.ag/amqp

.PHONY: fuzz
fuzz:
	go-fuzz-build -o ./go-fuzz/fuzz.zip $(PACKAGE)
	go-fuzz -bin ./go-fuzz/fuzz.zip -workdir go-fuzz

.PHONE: fuzzmarshal
fuzzmarshal:
	go-fuzz-build -o ./go-fuzz/fuzzmarshal.zip -func FuzzUnmarshal $(PACKAGE)
	go-fuzz -bin ./go-fuzz/fuzzmarshal.zip -workdir go-fuzz/marshal

.PHONY: fuzzclean
fuzzclean:
	rm -f ./go-fuzz{,marshal}/{crashers,suppressions}/*

.PHONY: test
test:
	go test -v -race ./...
