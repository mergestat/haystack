.PHONY: all clean build

all: clean build

build:
	go build -v -o .build/haystack cmd/haystack/*.go

clean:
	-rm -f .build/haystack
