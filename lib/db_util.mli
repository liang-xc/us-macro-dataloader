open! Core
open Async

val get_uri : unit -> string

val connect
  :  unit
  -> (Caqti_async.connection, [> Caqti_error.load_or_connect ]) result Deferred.t

val connect_exn : unit -> Caqti_async.connection Deferred.t
val failwith_err : [< Caqti_error.t ] -> 'a

val deferred_result_list_iter
  :  'a list
  -> f:(unit -> 'a -> (unit, 'b) result Deferred.t)
  -> (unit, 'b) result Deferred.t
