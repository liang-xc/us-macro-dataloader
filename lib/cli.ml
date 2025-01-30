open! Core
open Async

let update_auction_hist =
  Command.async
    ~summary:"UST auction historical data updater"
    (Command.Param.return Auction_hist.run_auction_hist_exn)
;;

let update_auction_upcoming =
  Command.async
    ~summary:"UST auction upcoming data updater"
    (Command.Param.return Auction_upcoming.run_auction_upcoming_exn)
;;

let command =
  Command.group
    ~summary:"US Fiscal data loaders"
    [ "auction-hist", update_auction_hist; "auction-upcoming", update_auction_upcoming ]
;;
