(** The Treasury Securities Upcoming Auctions Data dataset provides information on auction announcements.
    Each announcement includes what securities are being auctioned, the announcement date, the auction date and issue date.
    This data provides a notification of what Treasury Marketable securities will be announced and or auctioned in the upcoming week.
    API Docs can be found at: https://fiscaldata.treasury.gov/datasets/upcoming-auctions/treasury-securities-upcoming-auctions
    Available data starts from 2024-03-08 *)

open! Core
open Async
open Cohttp_async
open Json_util

type t =
  { record_date : string
  ; security_type : string
  ; security_term : string
  ; reopening : string
  ; cusip : string
  ; offering_amt : int option
  ; announcemt_date : string
  ; auction_date : string
  ; issue_date : string
  }
[@@deriving yojson]

let fetch_upcoming_auction start_date =
  let rec aux page_num lst =
    let endpoint =
      Uri.add_query_params'
        (Uri.of_string
           "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/upcoming_auctions")
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
    let auctions = List.map data ~f:t_of_yojson in
    if page_num < total_pages
    then aux (page_num + 1) (lst @ auctions)
    else return (lst @ auctions)
  in
  aux 1 []
;;

module Q = struct
  open Caqti_request.Infix
  open Caqti_type.Std

  let auction =
    let intro
      record_date
      security_type
      security_term
      reopening
      cusip
      offering_amt
      announcemt_date
      auction_date
      issue_date
      =
      { record_date
      ; security_type
      ; security_term
      ; reopening
      ; cusip
      ; offering_amt
      ; announcemt_date
      ; auction_date
      ; issue_date
      }
    in
    product intro
    @@ proj string (fun auction -> auction.record_date)
    @@ proj string (fun auction -> auction.security_type)
    @@ proj string (fun auction -> auction.security_term)
    @@ proj string (fun auction -> auction.reopening)
    @@ proj string (fun auction -> auction.cusip)
    @@ proj (option int) (fun auction -> auction.offering_amt)
    @@ proj string (fun auction -> auction.announcemt_date)
    @@ proj string (fun auction -> auction.auction_date)
    @@ proj string (fun auction -> auction.issue_date)
    @@ proj_end
  ;;

  let create_auction_upcoming =
    (unit ->. unit)
      {|
        CREATE TABLE IF NOT EXISTS auction_upcoming (
          record_date DATE,
          security_type TEXT,
          security_term TEXT,
          reopening BOOLEAN,
          cusip TEXT,
          offering_amt BIGINT NULL,
          announcemt_date DATE,
          auction_date DATE,
          issue_date DATE,
          PRIMARY KEY (record_date, cusip)
        )
    |}
  ;;

  let upsert_record =
    (auction ->. unit)
      {|
        INSERT INTO auction_upcoming (
            record_date,
            security_type,
            security_term,
            reopening,
            cusip,
            offering_amt,
            announcemt_date,
            auction_date,
            issue_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (record_date, cusip)
        DO UPDATE SET
            security_type = EXCLUDED.security_type,
            security_term = EXCLUDED.security_term,
            reopening = EXCLUDED.reopening,
            offering_amt = EXCLUDED.offering_amt,
            announcemt_date = EXCLUDED.announcemt_date,
            auction_date = EXCLUDED.auction_date,
            issue_date = EXCLUDED.issue_date
    |}
  ;;

  let max_hist_date = (unit ->! string) @@ "SELECT MAX(record_date) FROM auction_upcoming"
end

let create_auction_upcoming_tbl (module Db : Caqti_async.CONNECTION) =
  Db.exec Q.create_auction_upcoming ()
;;

let upsert_auction_hist (module Db : Caqti_async.CONNECTION) hist =
  Db.exec Q.upsert_record hist
;;

let get_max_hist_date (module Db : Caqti_async.CONNECTION) = Db.find Q.max_hist_date ()

let run_auction_upcoming_exn () =
  let run_auction_hist =
    let%bind conn = Db_util.connect_exn () in
    Deferred.Result.bind (create_auction_upcoming_tbl conn) ~f:(fun () ->
      let%bind date =
        match%map get_max_hist_date conn with
        | Ok d ->
          (match d with
           | "" ->
             let () =
               Log.Global.info "no record date available, using starting date 1979-11-01"
             in
             Date.of_string "2024-01-01"
           | _ ->
             let () = Log.Global.info "Last update date is %s" d in
             Date.of_string d)
        | Error err -> Db_util.failwith_err err
      in
      let%bind records = fetch_upcoming_auction date in
      let () = Log.Global.info "fetched %d new auction records" (List.length records) in
      (* use the following after Async v0.18.0 *)
      (* Deferred.Result.List.iter records ~f:(fun record -> insert_auction_hist conn record)) *)
      Db_util.deferred_result_list_iter records ~f:(fun () record ->
        upsert_auction_hist conn record))
  in
  match%map run_auction_hist with
  | Ok () -> Log.Global.info "Successfully update auction historical data"
  | Error err -> Db_util.failwith_err err
;;
