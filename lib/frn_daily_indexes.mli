open! Core
open Async

type t =
  { record_date : string
  ; frn : string
  ; cusip : string
  ; original_dated_date : string
  ; original_issue_date : string
  ; maturity_date : string
  ; spread : float
  ; start_of_accrual_period : string
  ; end_of_accrual_period : string
  ; daily_index : float
  ; daily_int_accrual_rate : float
  ; daily_accrued_int_per100 : float
  ; accr_int_per100_pmt_period : float
  }

val t_of_yojson : Yojson.Safe.t -> t
val yojson_of_t : t -> Yojson.Safe.t
val fetch_frn : Core.Date.t -> t list Deferred.t

val create_frn_idx_tbl
  :  Caqti_async.connection
  -> (unit, [> Caqti_error.call_or_retrieve ]) result Deferred.t

val upsert_frn_idx
  :  Caqti_async.connection
  -> t
  -> (unit, [> Caqti_error.call_or_retrieve ]) result Deferred.t

val get_max_hist_date
  :  Caqti_async.connection
  -> (string, [> Caqti_error.call_or_retrieve ]) result Deferred.t

val run_frn_daily_index_exn : unit -> unit Deferred.t
