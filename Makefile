REBAR ?= rebar3

.PHONY: test
all: compile

compile:
	$(REBAR) compile

dialyzer:
	$(REBAR) dialyzer

test:
	$(REBAR) ct

clean:
	$(REBAR) clean
	
