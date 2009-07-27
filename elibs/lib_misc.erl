-module(lib_misc).
-export([tohex/1]).

tohex(Data) when is_list(Data) ->
	lists:flatten([hex_byte(B) || B <- Data]);
tohex(Data) ->
	lists:flatten([hex_byte(B) || B <- binary_to_list(Data)]).

hex_byte(B) when B < 16#10 -> [$0 | erlang:integer_to_list(B, 16)];
hex_byte(B) -> erlang:integer_to_list(B, 16).
