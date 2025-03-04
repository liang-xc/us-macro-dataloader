(** The Treasury Securities Upcoming Auctions Data dataset provides information on auction announcements.
    Each announcement includes what securities are being auctioned, the announcement date, the auction date and issue date.
    This data provides a notification of what Treasury Marketable securities will be announced and or auctioned in the upcoming week.
    API Docs can be found at: https://fiscaldata.treasury.gov/datasets/upcoming-auctions/treasury-securities-upcoming-auctions
    Available data starts from 2024-03-08 *)

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
