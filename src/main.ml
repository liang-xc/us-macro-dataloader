open! Core
open Async

let update_auction_hist =
  Command.async
    ~summary:"UST auction historical data updater"
    (Command.Param.return Auction_hist.run_auction_hist_exn)
;;

let command =
  Command.group ~summary:"US Fiscal data loaders" [ "auction-hist", update_auction_hist ]
;;

let () = Command_unix.run command
