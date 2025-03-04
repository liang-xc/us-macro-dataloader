(** The Treasury Securities Auctions Data dataset provides data on announced and auctioned marketable Treasury securities.
    API Docs can be found at: https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/treasury-securities-auctions-data
    Available data starts from 1979-11-15 *)

open! Core
open Async

type t

val t_of_yojson : Yojson.Safe.t -> t
val yojson_of_t : t -> Yojson.Safe.t
val fetch_hist : Date.t -> t list Deferred.t

val create_auction_hist_tbl
  :  Caqti_async.connection
  -> (unit, [> Caqti_error.call_or_retrieve ]) result Deferred.t

val insert_auction_hist
  :  Caqti_async.connection
  -> t
  -> (unit, [> Caqti_error.call_or_retrieve ]) result Deferred.t

val get_max_hist_date
  :  Caqti_async.connection
  -> (string, [> Caqti_error.call_or_retrieve ]) result Deferred.t

val run_auction_hist_exn : unit -> unit Deferred.t
