REBAR ?= rebar3

.PHONY: test
all: compile

compile:
	$(REBAR) compile

edoc:
	$(REBAR) edoc

dialyzer:
	$(REBAR) dialyzer

test:
	$(REBAR) ct

clean:
	$(REBAR) clean
	
