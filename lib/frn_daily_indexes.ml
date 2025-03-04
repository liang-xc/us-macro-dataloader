open! Core
open Async
open Cohttp_async
open Json_util

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
[@@deriving yojson]

let fetch_frn start_date =
  let rec aux page_num lst =
    let endpoint =
      Uri.add_query_params'
        (Uri.of_string
           "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/frn_daily_indexes")
        [ "format", "json"
        ; "sort", "-record_date"
        ; "filter", "record_date:gt:" ^ Date.to_string start_date
        ; "page[number]", Int.to_string page_num
        ]
    in
    let%bind _, body = Client.get endpoint in
    let%bind str = Body.to_string body in
    let str = String.substr_replace_all ~pattern:{|"null"|} ~with_:"null" str in
    let j = Yojson.Safe.from_string str in
    let open Yojson.Safe.Util in
    let total_pages = j |> member "meta" |> member "total-pages" |> to_int in
    let data = j |> member "data" |> to_list in
    let frn_data = List.map data ~f:t_of_yojson in
    if page_num < total_pages
    then aux (page_num + 1) (lst @ frn_data)
    else return (lst @ frn_data)
  in
  aux 1 []
;;

module Q = struct
  open Caqti_request.Infix
  open Caqti_type.Std

  let frn_record =
    let intro
          record_date
          frn
          cusip
          original_dated_date
          original_issue_date
          maturity_date
          spread
          start_of_accrual_period
          end_of_accrual_period
          daily_index
          daily_int_accrual_rate
          daily_accrued_int_per100
          accr_int_per100_pmt_period
      =
      { record_date
      ; frn
      ; cusip
      ; original_dated_date
      ; original_issue_date
      ; maturity_date
      ; spread
      ; start_of_accrual_period
      ; end_of_accrual_period
      ; daily_index
      ; daily_int_accrual_rate
      ; daily_accrued_int_per100
      ; accr_int_per100_pmt_period
      }
    in
    product intro
    @@ proj string (fun frn_record -> frn_record.record_date)
    @@ proj string (fun frn_record -> frn_record.frn)
    @@ proj string (fun frn_record -> frn_record.cusip)
    @@ proj string (fun frn_record -> frn_record.original_dated_date)
    @@ proj string (fun frn_record -> frn_record.original_issue_date)
    @@ proj string (fun frn_record -> frn_record.maturity_date)
    @@ proj float (fun frn_record -> frn_record.spread)
    @@ proj string (fun frn_record -> frn_record.start_of_accrual_period)
    @@ proj string (fun frn_record -> frn_record.end_of_accrual_period)
    @@ proj float (fun frn_record -> frn_record.daily_index)
    @@ proj float (fun frn_record -> frn_record.daily_int_accrual_rate)
    @@ proj float (fun frn_record -> frn_record.daily_accrued_int_per100)
    @@ proj float (fun frn_record -> frn_record.accr_int_per100_pmt_period)
    @@ proj_end
  ;;

  let create_frn_idx =
    (unit ->. unit)
      {|
        CREATE TABLE IF NOT EXISTS frn_idx (
          record_date DATE,
          frn TEXT,
          cusip TEXT,
          original_dated_date DATE,
          original_issue_date DATE,
          maturity_date DATE,
          spread NUMERIC,
          start_of_accrual_period DATE,
          end_of_accrual_period DATE,
          daily_index NUMERIC,
          daily_int_accrual_rate NUMERIC,
          daily_accrued_int_per100 NUMERIC,
          accr_int_per100_pmt_period NUMERIC,
          PRIMARY KEY (record_date, cusip)
        )
      |}
  ;;

  let upsert_record =
    (frn_record ->. unit)
      {|
        INSERT INTO frn_idx (
            record_date,
            frn,
            cusip,
            original_dated_date,
            original_issue_date,
            maturity_date,
            spread,
            start_of_accrual_period,
            end_of_accrual_period,
            daily_index,
            daily_int_accrual_rate,
            daily_accrued_int_per100,
            accr_int_per100_pmt_period
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (record_date, cusip)
        DO UPDATE SET
            record_date = EXCLUDED.record_date,
            frn = EXCLUDED.frn,
            cusip = EXCLUDED.cusip,
            original_dated_date = EXCLUDED.original_dated_date,
            original_issue_date = EXCLUDED.original_issue_date,
            maturity_date = EXCLUDED.maturity_date,
            spread = EXCLUDED.spread,
            start_of_accrual_period = EXCLUDED.start_of_accrual_period,
            end_of_accrual_period = EXCLUDED.end_of_accrual_period,
            daily_index = EXCLUDED.daily_index,
            daily_int_accrual_rate = EXCLUDED.daily_int_accrual_rate,
            daily_accrued_int_per100 = EXCLUDED.daily_accrued_int_per100,
            accr_int_per100_pmt_period = EXCLUDED.accr_int_per100_pmt_period
    |}
  ;;

  let max_hist_date = (unit ->! string) @@ "SELECT MAX(record_date) FROM frn_idx"
end

let create_frn_idx_tbl (module Db : Caqti_async.CONNECTION) = Db.exec Q.create_frn_idx ()

let upsert_frn_idx (module Db : Caqti_async.CONNECTION) record =
  Db.exec Q.upsert_record record
;;

let get_max_hist_date (module Db : Caqti_async.CONNECTION) = Db.find Q.max_hist_date ()

let run_frn_daily_index_exn () =
  let run_auction_hist =
    let%bind conn = Db_util.connect_exn () in
    Deferred.Result.bind (create_frn_idx_tbl conn) ~f:(fun () ->
      let%bind date =
        match%map get_max_hist_date conn with
        | Ok d ->
          (match d with
           | "" ->
             let () =
               Log.Global.info "no record date available, using starting date 2023-09-27"
             in
             Date.of_string "2023-09-27"
           | _ ->
             let () = Log.Global.info "Last update date is %s" d in
             Date.of_string d)
        | Error err -> Db_util.failwith_err err
      in
      let%bind hist = fetch_frn date in
      let () = Log.Global.info "fetched %d new frn records" (List.length hist) in
      Db_util.deferred_result_list_iter hist ~f:(fun () record ->
        upsert_frn_idx conn record))
  in
  match%map run_auction_hist with
  | Ok () -> Log.Global.info "Successfully update frn data"
  | Error err -> Db_util.failwith_err err
;;
