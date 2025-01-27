open! Core
open Ppx_yojson_conv_lib.Yojson_conv.Primitives

let string_of_yojson = string_of_yojson
let yojson_of_string = yojson_of_string
let option_of_yojson = option_of_yojson
let yojson_of_option = yojson_of_option

let int_of_yojson yojson =
  match yojson with
  | `Int v -> v
  | `String str -> Int.of_string str
  | _ ->
    Ppx_yojson_conv_lib.Yojson_conv.of_yojson_error "int_of_yojson: integer needed" yojson
;;

let yojson_of_int = yojson_of_int

let float_of_yojson yojson =
  match yojson with
  | `Float v -> v
  | `Int i -> float_of_int i
  | `Intlit str -> float_of_string str
  | `String str -> float_of_string str
  | _ ->
    Ppx_yojson_conv_lib.Yojson_conv.of_yojson_error "float_of_yojson: float needed" yojson
;;

let yojson_of_float = yojson_of_float
