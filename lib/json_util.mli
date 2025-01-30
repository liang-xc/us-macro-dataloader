open! Core

val string_of_yojson : Yojson.Safe.t -> string
val yojson_of_string : string -> Yojson.Safe.t
val option_of_yojson : (Yojson.Safe.t -> 'a) -> Yojson.Safe.t -> 'a option
val yojson_of_option : ('a -> Yojson.Safe.t) -> 'a option -> Yojson.Safe.t
val int_of_yojson : Yojson.Safe.t -> int
val yojson_of_int : int -> Yojson.Safe.t
val float_of_yojson : Yojson.Safe.t -> float
val yojson_of_float : float -> Yojson.Safe.t
