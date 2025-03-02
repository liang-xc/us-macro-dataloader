open! Core

let%expect_test _ =
  let b = true in
  Sexp.to_string_hum ([%sexp_of: bool] b) |> print_endline;
  [%expect {| true |}]
;;
