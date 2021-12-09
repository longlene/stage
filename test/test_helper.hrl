-include_lib("eunit/include/eunit.hrl").

-define(assertReceive(Guard), ?assert(receive Guard -> true after 5000 -> false end)).
-define(assertReceived(Guard), ?assert(receive Guard -> true after 0 -> false end)).
-define(refuteReceived(Guard), ?assertNot(receive Guard -> true after 0 -> false end)).

