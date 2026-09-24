.PHONY: all parser clean

all: fmt parser

parser: parser.go hintparser.go

%arser.go: prefix = $(@:parser.go=)
%arser.go: %arser.y bin/goyacc
	@echo "bin/goyacc -o $@ -p yy$(prefix) -t $(prefix)Parser $<"
	@bin/goyacc -o $@ -p yy$(prefix) -t $(prefix)Parser $< || ( rm -f $@ && echo 'Please check y.output for more information' && exit 1 )
	@rm -f y.output

%arser_golden.y: %arser.y
	@bin/goyacc -fmt -fmtout $@ $<
	@(git diff --no-index --exit-code $< $@ && rm $@) || (mv $@ $< && >&2 echo "formatted $<" && exit 1)

# goyacc is a nested Go module (its own go.mod) so its modernc.org
# dependencies stay out of spirit's module graph.
bin/goyacc: goyacc/*.go
	go build -C goyacc -o ../bin/goyacc .

fmt: bin/goyacc parser_golden.y hintparser_golden.y
	@echo "gofmt (simplify)"
	@gofmt -s -l -w . 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

clean:
	go clean -i ./...
	rm -rf *.out
	rm -f parser.go hintparser.go
