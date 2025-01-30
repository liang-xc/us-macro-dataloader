open! Core
open Async

type t

val t_of_yojson : Yojson.Safe.t -> t
val yojson_of_t : t -> Yojson.Safe.t
val fetch_upcoming_auction : Date.t -> t list Deferred.t

val create_auction_upcoming_tbl
  :  Caqti_async.connection
  -> (unit, [> Caqti_error.call_or_retrieve ]) result Deferred.t

val upsert_auction_hist
  :  Caqti_async.connection
  -> t
  -> (unit, [> Caqti_error.call_or_retrieve ]) result Deferred.t

val get_max_hist_date
  :  Caqti_async.connection
  -> (string, [> Caqti_error.call_or_retrieve ]) result Deferred.t

val run_auction_upcoming_exn : unit -> unit Deferred.t
