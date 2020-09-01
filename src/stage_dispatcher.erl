-module(stage_dispatcher).

-type keyword() :: [{atom(), any()}].

-callback init(keyword()) -> {ok, any()}.

-callback subscribe(keyword(), {pid(), reference()}, term()) -> {ok, non_neg_integer(), term()} | {error, term()}.

-callback cancel({pid(), reference()}, term()) -> {ok, non_neg_integer(), term()}.

-callback ask(pos_integer(), {pid(), reference()}, term()) -> {ok, non_neg_integer(), term()}.

-callback dispatch(nonempty_list(term()), pos_integer(), term()) -> {ok, [term()], term()}.

-callback info(term(), term()) -> {ok, term()}.
