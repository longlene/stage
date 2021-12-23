REBAR ?= rebar3

.PHONY: test
all: compile

compile:
	$(REBAR) compile

doc:
	$(REBAR) edoc

dialyzer:
	$(REBAR) dialyzer

test:
	$(REBAR) ct

clean:
	$(REBAR) clean
	
