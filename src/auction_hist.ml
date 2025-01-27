(** The Treasury Securities Auctions Data dataset provides data on announced and auctioned marketable Treasury securities.
    API Docs can be found at: https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/treasury-securities-auctions-data
    Available data starts from 1979-11-15 *)

open! Core
open Async
open Cohttp_async
open Json_util

type t =
  { record_date : string
  ; cusip : string
  ; security_type : string
  ; security_term : string option
  ; auction_date : string
  ; issue_date : string
  ; maturity_date : string
  ; price_per100 : string option
  ; accrued_int_per100 : float option
  ; accrued_int_per1000 : float option
  ; adj_accrued_int_per1000 : float option
  ; adj_price : float option
  ; allocation_pctage : float option
  ; allocation_pctage_decimals : float option
  ; announcemtd_cusip : string option
  ; announcemt_date : string option
  ; auction_format : string option
  ; avg_med_discnt_rate : float option
  ; avg_med_investment_rate : float option
  ; avg_med_price : string option
  ; avg_med_discnt_margin : float option
  ; avg_med_yield : float option
  ; back_dated : string option
  ; back_dated_date : string option
  ; bid_to_cover_ratio : float option
  ; callable : string option
  ; call_date : string option
  ; called_date : string option
  ; cash_management_bill_cmb : string option
  ; closing_time_comp : string option
  ; closing_time_noncomp : string option
  ; comp_accepted : float option
  ; comp_bid_decimals : float option
  ; comp_tendered : float option
  ; comp_tenders_accepted : string option
  ; corpus_cusip : string option
  ; cpi_base_reference_period : string option
  ; currently_outstanding : float option
  ; dated_date : string option
  ; direct_bidder_accepted : float option
  ; direct_bidder_tendered : float option
  ; est_pub_held_mat_by_type_amt : float option
  ; fima_included : string option
  ; fima_noncomp_accepted : float option
  ; fima_noncomp_tendered : float option
  ; first_int_period : string option
  ; first_int_payment_date : string option
  ; floating_rate : string option
  ; frn_index_determination_date : string option
  ; frn_index_determination_rate : float option
  ; high_discnt_rate : float option
  ; high_investment_rate : float option
  ; high_price : string option
  ; high_discnt_margin : float option
  ; high_yield : float option
  ; index_ratio_on_issue_date : float option
  ; indirect_bidder_accepted : float option
  ; indirect_bidder_tendered : float option
  ; int_payment_frequency : string option
  ; int_rate : float option
  ; low_discnt_rate : float option
  ; low_investment_rate : float option
  ; low_price : string option
  ; low_discnt_margin : float option
  ; low_yield : float option
  ; mat_date : string option
  ; max_comp_award : float option
  ; max_noncomp_award : float option
  ; max_single_bid : float option
  ; min_bid_amt : float option
  ; min_strip_amt : float option
  ; min_to_issue : float option
  ; multiples_to_bid : float option
  ; multiples_to_issue : float option
  ; nlp_exclusion_amt : float option
  ; nlp_reporting_threshold : float option
  ; noncomp_accepted : float option
  ; noncomp_tenders_accepted : string option
  ; offering_amt : float option
  ; original_cusip : string option
  ; original_dated_date : string option
  ; original_issue_date : string option
  ; original_security_term : string option
  ; pdf_filenm_announcemt : string option
  ; pdf_filenm_comp_results : string option
  ; pdf_filenm_noncomp_results : string option
  ; primary_dealer_accepted : float option
  ; primary_dealer_tendered : float option
  ; ref_cpi_on_dated_date : float option
  ; ref_cpi_on_issue_date : float option
  ; reopening : string option
  ; security_term_day_month : string option
  ; security_term_week_year : string option
  ; series : string option
  ; soma_accepted : float option
  ; soma_holdings : float option
  ; soma_included : string option
  ; soma_tendered : float option
  ; spread : float option
  ; std_int_payment_per1000 : float option
  ; strippable : string option
  ; tiin_conversion_factor_per1000 : float option
  ; total_accepted : float option
  ; total_tendered : float option
  ; treas_retail_accepted : float option
  ; treas_retail_tenders_accepted : string option
  ; unadj_accrued_int_per1000 : float option
  ; unadj_price : float option
  ; xml_filenm_announcemt : string option
  ; xml_filenm_comp_results : string option
  ; inflation_index_security : string option
  ; tint_cusip_1 : string option
  ; tint_cusip_2 : string option
  }
[@@deriving yojson]

let fetch_hist start_date =
  let rec aux page_num lst =
    let endpoint =
      Uri.add_query_params'
        (Uri.of_string
           "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query")
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
      cusip
      security_type
      security_term
      auction_date
      issue_date
      maturity_date
      price_per100
      accrued_int_per100
      accrued_int_per1000
      adj_accrued_int_per1000
      adj_price
      allocation_pctage
      allocation_pctage_decimals
      announcemtd_cusip
      announcemt_date
      auction_format
      avg_med_discnt_rate
      avg_med_investment_rate
      avg_med_price
      avg_med_discnt_margin
      avg_med_yield
      back_dated
      back_dated_date
      bid_to_cover_ratio
      callable
      call_date
      called_date
      cash_management_bill_cmb
      closing_time_comp
      closing_time_noncomp
      comp_accepted
      comp_bid_decimals
      comp_tendered
      comp_tenders_accepted
      corpus_cusip
      cpi_base_reference_period
      currently_outstanding
      dated_date
      direct_bidder_accepted
      direct_bidder_tendered
      est_pub_held_mat_by_type_amt
      fima_included
      fima_noncomp_accepted
      fima_noncomp_tendered
      first_int_period
      first_int_payment_date
      floating_rate
      frn_index_determination_date
      frn_index_determination_rate
      high_discnt_rate
      high_investment_rate
      high_price
      high_discnt_margin
      high_yield
      index_ratio_on_issue_date
      indirect_bidder_accepted
      indirect_bidder_tendered
      int_payment_frequency
      int_rate
      low_discnt_rate
      low_investment_rate
      low_price
      low_discnt_margin
      low_yield
      mat_date
      max_comp_award
      max_noncomp_award
      max_single_bid
      min_bid_amt
      min_strip_amt
      min_to_issue
      multiples_to_bid
      multiples_to_issue
      nlp_exclusion_amt
      nlp_reporting_threshold
      noncomp_accepted
      noncomp_tenders_accepted
      offering_amt
      original_cusip
      original_dated_date
      original_issue_date
      original_security_term
      pdf_filenm_announcemt
      pdf_filenm_comp_results
      pdf_filenm_noncomp_results
      primary_dealer_accepted
      primary_dealer_tendered
      ref_cpi_on_dated_date
      ref_cpi_on_issue_date
      reopening
      security_term_day_month
      security_term_week_year
      series
      soma_accepted
      soma_holdings
      soma_included
      soma_tendered
      spread
      std_int_payment_per1000
      strippable
      tiin_conversion_factor_per1000
      total_accepted
      total_tendered
      treas_retail_accepted
      treas_retail_tenders_accepted
      unadj_accrued_int_per1000
      unadj_price
      xml_filenm_announcemt
      xml_filenm_comp_results
      inflation_index_security
      tint_cusip_1
      tint_cusip_2
      =
      { record_date
      ; cusip
      ; security_type
      ; security_term
      ; auction_date
      ; issue_date
      ; maturity_date
      ; price_per100
      ; accrued_int_per100
      ; accrued_int_per1000
      ; adj_accrued_int_per1000
      ; adj_price
      ; allocation_pctage
      ; allocation_pctage_decimals
      ; announcemtd_cusip
      ; announcemt_date
      ; auction_format
      ; avg_med_discnt_rate
      ; avg_med_investment_rate
      ; avg_med_price
      ; avg_med_discnt_margin
      ; avg_med_yield
      ; back_dated
      ; back_dated_date
      ; bid_to_cover_ratio
      ; callable
      ; call_date
      ; called_date
      ; cash_management_bill_cmb
      ; closing_time_comp
      ; closing_time_noncomp
      ; comp_accepted
      ; comp_bid_decimals
      ; comp_tendered
      ; comp_tenders_accepted
      ; corpus_cusip
      ; cpi_base_reference_period
      ; currently_outstanding
      ; dated_date
      ; direct_bidder_accepted
      ; direct_bidder_tendered
      ; est_pub_held_mat_by_type_amt
      ; fima_included
      ; fima_noncomp_accepted
      ; fima_noncomp_tendered
      ; first_int_period
      ; first_int_payment_date
      ; floating_rate
      ; frn_index_determination_date
      ; frn_index_determination_rate
      ; high_discnt_rate
      ; high_investment_rate
      ; high_price
      ; high_discnt_margin
      ; high_yield
      ; index_ratio_on_issue_date
      ; indirect_bidder_accepted
      ; indirect_bidder_tendered
      ; int_payment_frequency
      ; int_rate
      ; low_discnt_rate
      ; low_investment_rate
      ; low_price
      ; low_discnt_margin
      ; low_yield
      ; mat_date
      ; max_comp_award
      ; max_noncomp_award
      ; max_single_bid
      ; min_bid_amt
      ; min_strip_amt
      ; min_to_issue
      ; multiples_to_bid
      ; multiples_to_issue
      ; nlp_exclusion_amt
      ; nlp_reporting_threshold
      ; noncomp_accepted
      ; noncomp_tenders_accepted
      ; offering_amt
      ; original_cusip
      ; original_dated_date
      ; original_issue_date
      ; original_security_term
      ; pdf_filenm_announcemt
      ; pdf_filenm_comp_results
      ; pdf_filenm_noncomp_results
      ; primary_dealer_accepted
      ; primary_dealer_tendered
      ; ref_cpi_on_dated_date
      ; ref_cpi_on_issue_date
      ; reopening
      ; security_term_day_month
      ; security_term_week_year
      ; series
      ; soma_accepted
      ; soma_holdings
      ; soma_included
      ; soma_tendered
      ; spread
      ; std_int_payment_per1000
      ; strippable
      ; tiin_conversion_factor_per1000
      ; total_accepted
      ; total_tendered
      ; treas_retail_accepted
      ; treas_retail_tenders_accepted
      ; unadj_accrued_int_per1000
      ; unadj_price
      ; xml_filenm_announcemt
      ; xml_filenm_comp_results
      ; inflation_index_security
      ; tint_cusip_1
      ; tint_cusip_2
      }
    in
    product intro
    @@ proj string (fun auction -> auction.record_date)
    @@ proj string (fun auction -> auction.cusip)
    @@ proj string (fun auction -> auction.security_type)
    @@ proj (option string) (fun auction -> auction.security_term)
    @@ proj string (fun auction -> auction.auction_date)
    @@ proj string (fun auction -> auction.issue_date)
    @@ proj string (fun auction -> auction.maturity_date)
    @@ proj (option string) (fun auction -> auction.price_per100)
    @@ proj (option float) (fun auction -> auction.accrued_int_per100)
    @@ proj (option float) (fun auction -> auction.accrued_int_per1000)
    @@ proj (option float) (fun auction -> auction.adj_accrued_int_per1000)
    @@ proj (option float) (fun auction -> auction.adj_price)
    @@ proj (option float) (fun auction -> auction.allocation_pctage)
    @@ proj (option float) (fun auction -> auction.allocation_pctage_decimals)
    @@ proj (option string) (fun auction -> auction.announcemtd_cusip)
    @@ proj (option string) (fun auction -> auction.announcemt_date)
    @@ proj (option string) (fun auction -> auction.auction_format)
    @@ proj (option float) (fun auction -> auction.avg_med_discnt_rate)
    @@ proj (option float) (fun auction -> auction.avg_med_investment_rate)
    @@ proj (option string) (fun auction -> auction.avg_med_price)
    @@ proj (option float) (fun auction -> auction.avg_med_discnt_margin)
    @@ proj (option float) (fun auction -> auction.avg_med_yield)
    @@ proj (option string) (fun auction -> auction.back_dated)
    @@ proj (option string) (fun auction -> auction.back_dated_date)
    @@ proj (option float) (fun auction -> auction.bid_to_cover_ratio)
    @@ proj (option string) (fun auction -> auction.callable)
    @@ proj (option string) (fun auction -> auction.call_date)
    @@ proj (option string) (fun auction -> auction.called_date)
    @@ proj (option string) (fun auction -> auction.cash_management_bill_cmb)
    @@ proj (option string) (fun auction -> auction.closing_time_comp)
    @@ proj (option string) (fun auction -> auction.closing_time_noncomp)
    @@ proj (option float) (fun auction -> auction.comp_accepted)
    @@ proj (option float) (fun auction -> auction.comp_bid_decimals)
    @@ proj (option float) (fun auction -> auction.comp_tendered)
    @@ proj (option string) (fun auction -> auction.comp_tenders_accepted)
    @@ proj (option string) (fun auction -> auction.corpus_cusip)
    @@ proj (option string) (fun auction -> auction.cpi_base_reference_period)
    @@ proj (option float) (fun auction -> auction.currently_outstanding)
    @@ proj (option string) (fun auction -> auction.dated_date)
    @@ proj (option float) (fun auction -> auction.direct_bidder_accepted)
    @@ proj (option float) (fun auction -> auction.direct_bidder_tendered)
    @@ proj (option float) (fun auction -> auction.est_pub_held_mat_by_type_amt)
    @@ proj (option string) (fun auction -> auction.fima_included)
    @@ proj (option float) (fun auction -> auction.fima_noncomp_accepted)
    @@ proj (option float) (fun auction -> auction.fima_noncomp_tendered)
    @@ proj (option string) (fun auction -> auction.first_int_period)
    @@ proj (option string) (fun auction -> auction.first_int_payment_date)
    @@ proj (option string) (fun auction -> auction.floating_rate)
    @@ proj (option string) (fun auction -> auction.frn_index_determination_date)
    @@ proj (option float) (fun auction -> auction.frn_index_determination_rate)
    @@ proj (option float) (fun auction -> auction.high_discnt_rate)
    @@ proj (option float) (fun auction -> auction.high_investment_rate)
    @@ proj (option string) (fun auction -> auction.high_price)
    @@ proj (option float) (fun auction -> auction.high_discnt_margin)
    @@ proj (option float) (fun auction -> auction.high_yield)
    @@ proj (option float) (fun auction -> auction.index_ratio_on_issue_date)
    @@ proj (option float) (fun auction -> auction.indirect_bidder_accepted)
    @@ proj (option float) (fun auction -> auction.indirect_bidder_tendered)
    @@ proj (option string) (fun auction -> auction.int_payment_frequency)
    @@ proj (option float) (fun auction -> auction.int_rate)
    @@ proj (option float) (fun auction -> auction.low_discnt_rate)
    @@ proj (option float) (fun auction -> auction.low_investment_rate)
    @@ proj (option string) (fun auction -> auction.low_price)
    @@ proj (option float) (fun auction -> auction.low_discnt_margin)
    @@ proj (option float) (fun auction -> auction.low_yield)
    @@ proj (option string) (fun auction -> auction.mat_date)
    @@ proj (option float) (fun auction -> auction.max_comp_award)
    @@ proj (option float) (fun auction -> auction.max_noncomp_award)
    @@ proj (option float) (fun auction -> auction.max_single_bid)
    @@ proj (option float) (fun auction -> auction.min_bid_amt)
    @@ proj (option float) (fun auction -> auction.min_strip_amt)
    @@ proj (option float) (fun auction -> auction.min_to_issue)
    @@ proj (option float) (fun auction -> auction.multiples_to_bid)
    @@ proj (option float) (fun auction -> auction.multiples_to_issue)
    @@ proj (option float) (fun auction -> auction.nlp_exclusion_amt)
    @@ proj (option float) (fun auction -> auction.nlp_reporting_threshold)
    @@ proj (option float) (fun auction -> auction.noncomp_accepted)
    @@ proj (option string) (fun auction -> auction.noncomp_tenders_accepted)
    @@ proj (option float) (fun auction -> auction.offering_amt)
    @@ proj (option string) (fun auction -> auction.original_cusip)
    @@ proj (option string) (fun auction -> auction.original_dated_date)
    @@ proj (option string) (fun auction -> auction.original_issue_date)
    @@ proj (option string) (fun auction -> auction.original_security_term)
    @@ proj (option string) (fun auction -> auction.pdf_filenm_announcemt)
    @@ proj (option string) (fun auction -> auction.pdf_filenm_comp_results)
    @@ proj (option string) (fun auction -> auction.pdf_filenm_noncomp_results)
    @@ proj (option float) (fun auction -> auction.primary_dealer_accepted)
    @@ proj (option float) (fun auction -> auction.primary_dealer_tendered)
    @@ proj (option float) (fun auction -> auction.ref_cpi_on_dated_date)
    @@ proj (option float) (fun auction -> auction.ref_cpi_on_issue_date)
    @@ proj (option string) (fun auction -> auction.reopening)
    @@ proj (option string) (fun auction -> auction.security_term_day_month)
    @@ proj (option string) (fun auction -> auction.security_term_week_year)
    @@ proj (option string) (fun auction -> auction.series)
    @@ proj (option float) (fun auction -> auction.soma_accepted)
    @@ proj (option float) (fun auction -> auction.soma_holdings)
    @@ proj (option string) (fun auction -> auction.soma_included)
    @@ proj (option float) (fun auction -> auction.soma_tendered)
    @@ proj (option float) (fun auction -> auction.spread)
    @@ proj (option float) (fun auction -> auction.std_int_payment_per1000)
    @@ proj (option string) (fun auction -> auction.strippable)
    @@ proj (option float) (fun auction -> auction.tiin_conversion_factor_per1000)
    @@ proj (option float) (fun auction -> auction.total_accepted)
    @@ proj (option float) (fun auction -> auction.total_tendered)
    @@ proj (option float) (fun auction -> auction.treas_retail_accepted)
    @@ proj (option string) (fun auction -> auction.treas_retail_tenders_accepted)
    @@ proj (option float) (fun auction -> auction.unadj_accrued_int_per1000)
    @@ proj (option float) (fun auction -> auction.unadj_price)
    @@ proj (option string) (fun auction -> auction.xml_filenm_announcemt)
    @@ proj (option string) (fun auction -> auction.xml_filenm_comp_results)
    @@ proj (option string) (fun auction -> auction.inflation_index_security)
    @@ proj (option string) (fun auction -> auction.tint_cusip_1)
    @@ proj (option string) (fun auction -> auction.tint_cusip_2)
    @@ proj_end
  ;;

  let create_auction_hist =
    (unit ->. unit)
    @@ {|
      CREATE TABLE IF NOT EXISTS auction_hist (
        record_date DATE NOT NULL,
        cusip TEXT NOT NULL,
        security_type TEXT NOT NULL,
        security_term TEXT,
        auction_date DATE NOT NULL,
        issue_date DATE NOT NULL,
        maturity_date DATE NOT NULL,
        price_per100 TEXT,
        accrued_int_per100 NUMERIC,
        accrued_int_per1000 NUMERIC,
        adj_accrued_int_per1000 NUMERIC,
        adj_price NUMERIC,
        allocation_pctage NUMERIC,
        allocation_pctage_decimals NUMERIC,
        announcemtd_cusip TEXT,
        announcemt_date DATE,
        auction_format TEXT,
        avg_med_discnt_rate NUMERIC,
        avg_med_investment_rate NUMERIC,
        avg_med_price NUMERIC,
        avg_med_discnt_margin NUMERIC,
        avg_med_yield NUMERIC,
        back_dated BOOLEAN,
        back_dated_date DATE,
        bid_to_cover_ratio NUMERIC,
        callable BOOLEAN,
        call_date DATE,
        called_date DATE,
        cash_management_bill_cmb BOOLEAN,
        closing_time_comp TEXT,
        closing_time_noncomp TEXT,
        comp_accepted NUMERIC,
        comp_bid_decimals NUMERIC,
        comp_tendered NUMERIC,
        comp_tenders_accepted BOOLEAN,
        corpus_cusip TEXT,
        cpi_base_reference_period TEXT,
        currently_outstanding NUMERIC,
        dated_date DATE,
        direct_bidder_accepted NUMERIC,
        direct_bidder_tendered NUMERIC,
        est_pub_held_mat_by_type_amt NUMERIC,
        fima_included BOOLEAN,
        fima_noncomp_accepted NUMERIC,
        fima_noncomp_tendered NUMERIC,
        first_int_period TEXT,
        first_int_payment_date DATE,
        floating_rate BOOLEAN,
        frn_index_determination_date DATE,
        frn_index_determination_rate NUMERIC,
        high_discnt_rate NUMERIC,
        high_investment_rate NUMERIC,
        high_price TEXT,
        high_discnt_margin NUMERIC,
        high_yield NUMERIC,
        index_ratio_on_issue_date NUMERIC,
        indirect_bidder_accepted NUMERIC,
        indirect_bidder_tendered NUMERIC,
        int_payment_frequency TEXT,
        int_rate NUMERIC,
        low_discnt_rate NUMERIC,
        low_investment_rate NUMERIC,
        low_price TEXT,
        low_discnt_margin NUMERIC,
        low_yield NUMERIC,
        mat_date DATE,
        max_comp_award NUMERIC,
        max_noncomp_award NUMERIC,
        max_single_bid NUMERIC,
        min_bid_amt NUMERIC,
        min_strip_amt NUMERIC,
        min_to_issue NUMERIC,
        multiples_to_bid NUMERIC,
        multiples_to_issue NUMERIC,
        nlp_exclusion_amt NUMERIC,
        nlp_reporting_threshold NUMERIC,
        noncomp_accepted NUMERIC,
        noncomp_tenders_accepted BOOLEAN,
        offering_amt NUMERIC,
        original_cusip TEXT,
        original_dated_date DATE,
        original_issue_date DATE,
        original_security_term TEXT,
        pdf_filenm_announcemt TEXT,
        pdf_filenm_comp_results TEXT,
        pdf_filenm_noncomp_results TEXT,
        primary_dealer_accepted NUMERIC,
        primary_dealer_tendered NUMERIC,
        ref_cpi_on_dated_date NUMERIC,
        ref_cpi_on_issue_date NUMERIC,
        reopening BOOLEAN,
        security_term_day_month TEXT,
        security_term_week_year TEXT,
        series TEXT,
        soma_accepted NUMERIC,
        soma_holdings NUMERIC,
        soma_included BOOLEAN,
        soma_tendered NUMERIC,
        spread NUMERIC,
        std_int_payment_per1000 NUMERIC,
        strippable BOOLEAN,
        tiin_conversion_factor_per1000 NUMERIC,
        total_accepted NUMERIC,
        total_tendered NUMERIC,
        treas_retail_accepted NUMERIC,
        treas_retail_tenders_accepted BOOLEAN,
        unadj_accrued_int_per1000 NUMERIC,
        unadj_price NUMERIC,
        xml_filenm_announcemt TEXT,
        xml_filenm_comp_results TEXT,
        inflation_index_security BOOLEAN,
        tint_cusip_1 TEXT,
        tint_cusip_2 TEXT,
        PRIMARY KEY (record_date, cusip)
      )
    |}
  ;;

  let insert_auction_hist =
    (auction ->. unit)
    @@ {|
      INSERT INTO auction_hist (record_date, cusip, security_type, security_term, auction_date, issue_date, maturity_date, price_per100, accrued_int_per100, accrued_int_per1000, adj_accrued_int_per1000, adj_price, allocation_pctage, allocation_pctage_decimals, announcemtd_cusip, announcemt_date, auction_format, avg_med_discnt_rate, avg_med_investment_rate, avg_med_price, avg_med_discnt_margin, avg_med_yield, back_dated, back_dated_date, bid_to_cover_ratio, callable, call_date, called_date, cash_management_bill_cmb, closing_time_comp, closing_time_noncomp, comp_accepted, comp_bid_decimals, comp_tendered, comp_tenders_accepted, corpus_cusip, cpi_base_reference_period, currently_outstanding, dated_date, direct_bidder_accepted, direct_bidder_tendered, est_pub_held_mat_by_type_amt, fima_included, fima_noncomp_accepted, fima_noncomp_tendered, first_int_period, first_int_payment_date, floating_rate, frn_index_determination_date, frn_index_determination_rate, high_discnt_rate, high_investment_rate, high_price, high_discnt_margin, high_yield, index_ratio_on_issue_date, indirect_bidder_accepted, indirect_bidder_tendered, int_payment_frequency, int_rate, low_discnt_rate, low_investment_rate, low_price, low_discnt_margin, low_yield, mat_date, max_comp_award, max_noncomp_award, max_single_bid, min_bid_amt, min_strip_amt, min_to_issue, multiples_to_bid, multiples_to_issue, nlp_exclusion_amt, nlp_reporting_threshold, noncomp_accepted, noncomp_tenders_accepted, offering_amt, original_cusip, original_dated_date, original_issue_date, original_security_term, pdf_filenm_announcemt, pdf_filenm_comp_results, pdf_filenm_noncomp_results, primary_dealer_accepted, primary_dealer_tendered, ref_cpi_on_dated_date, ref_cpi_on_issue_date, reopening, security_term_day_month, security_term_week_year, series, soma_accepted, soma_holdings, soma_included, soma_tendered, spread, std_int_payment_per1000, strippable, tiin_conversion_factor_per1000, total_accepted, total_tendered, treas_retail_accepted, treas_retail_tenders_accepted, unadj_accrued_int_per1000, unadj_price, xml_filenm_announcemt, xml_filenm_comp_results, inflation_index_security, tint_cusip_1, tint_cusip_2)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    |}
  ;;

  let max_hist_date = (unit ->! string) @@ "SELECT MAX(record_date) FROM auction_hist"
end

let create_auction_hist_tbl (module Db : Caqti_async.CONNECTION) =
  Db.exec Q.create_auction_hist ()
;;

let insert_auction_hist (module Db : Caqti_async.CONNECTION) hist =
  Db.exec Q.insert_auction_hist hist
;;

let get_max_hist_date (module Db : Caqti_async.CONNECTION) = Db.find Q.max_hist_date ()

let run_auction_hist_exn () =
  let run_auction_hist =
    let%bind conn = Db_util.connect_exn () in
    Deferred.Result.bind (create_auction_hist_tbl conn) ~f:(fun () ->
      let%bind date =
        match%map get_max_hist_date conn with
        | Ok d ->
          (match d with
           | "" ->
             let () =
               Log.Global.info "no record date available, using starting date 1979-11-01"
             in
             Date.of_string "1979-011-01"
           | _ ->
             let () = Log.Global.info "Last update date is %s" d in
             Date.of_string d)
        | Error err -> Db_util.failwith_err err
      in
      let%bind hist = fetch_hist date in
      let () = Log.Global.info "fetched %d new auction records" (List.length hist) in
      Deferred.Result.List.iter hist ~f:(fun record -> insert_auction_hist conn record))
  in
  match%map run_auction_hist with
  | Ok () -> Log.Global.info "Successfully update auction historical data"
  | Error err -> Db_util.failwith_err err
;;
