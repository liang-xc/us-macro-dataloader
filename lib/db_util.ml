open! Core
open Async

let get_uri () = Sys.getenv_exn "POSTGRES_URL"

let connect () =
  let uri = get_uri () in
  Caqti_async.connect (Uri.of_string uri)
;;

let connect_exn () =
  match%map connect () with
  | Error err ->
    let msg =
      Printf.sprintf
        "Abort! We could not get a connection. (err=%s)\n"
        (Caqti_error.show err)
    in
    failwith msg
  | Ok conn -> conn
;;

let failwith_err err =
  let msg = Printf.sprintf "Database Error: (err=%s)\n" (Caqti_error.show err) in
  failwith msg
;;

(** this is a placehodler for Deferred.Result.List.iter before Async v0.18.0 *)
let deferred_result_list_iter list ~f =
  let open Deferred.Result.Let_syntax in
  let rec loop acc = function
    | [] -> return acc
    | hd :: tl ->
      let%bind acc = f acc hd in
      loop acc tl
  in
  loop () list
;;
