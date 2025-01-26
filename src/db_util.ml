open! Core
open Async

let get_uri () =
  let open Option.Let_syntax in
  let env_vars =
    let%bind pg_host = Sys.getenv "PGHOST" in
    let%bind pg_port = Sys.getenv "PGPORT" in
    let%bind pg_database = Sys.getenv "PGDATABASE" in
    let%bind pg_username = Sys.getenv "PGUSERNAME" in
    let%bind pg_password = Sys.getenv "PGPASSWORD" in
    Some (pg_host, pg_port, pg_database, pg_username, pg_password)
  in
  match env_vars with
  | Some (pg_host, pg_port, pg_database, pg_username, pg_password) ->
    Printf.sprintf
      "postgresql://%s:%s@%s:%s/%s"
      pg_username
      pg_password
      pg_host
      pg_port
      pg_database
  | None -> "postgresql://postgres:password@localhost:5432/postgres"
;;

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
