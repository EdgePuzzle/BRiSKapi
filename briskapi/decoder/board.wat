;; The board totals of a BRiSK decoder's price ladder, summed in its own memory (see decoder.cjs, BoardReader).
;; Assembled with WABT's wat2wasm; decoder.cjs embeds the bytes (BOARD_KERNEL), and a test checks they match.
;;
;; The ladder is a std::deque of 72-byte levels, 56 to a block: `map` is the deque's block map, `start` its
;; first slot, `size` its length. A level holds the quantities (f64) of its bids and closing-condition bids at
;; +8 and +16, their order counts (u32) at +24 and +28, and the same for the asks at +40, +48, +56 and +60.
;; sum() adds each up over every level, in order, and stores them at `out`: the bid quantities (f64x2), the ask
;; quantities (f64x2), the bid order counts (i64x2), the ask order counts (i64x2); then (i32) the highest slot
;; with a bid quantity, and with a closing-condition one, and the lowest with an ask quantity, and with a
;; closing-condition one (-1: none), where a book's best levels begin. A quantity counts if it is not zero (or NaN).
(module
  (import "env" "memory" (memory 1))
  (func (export "sum") (param $map i32) (param $start i32) (param $size i32) (param $out i32)
    (local $i i32) (local $k i32) (local $run i32) (local $e i32) (local $end i32) (local $slot i32) (local $q f64)
    (local $bids v128) (local $asks v128) (local $bid_orders v128) (local $ask_orders v128)
    (local $top_bid i32) (local $top_closing_bid i32) (local $low_ask i32) (local $low_closing_ask i32)
    (local.set $top_bid (i32.const -1)) (local.set $top_closing_bid (i32.const -1))
    (local.set $low_ask (i32.const -1)) (local.set $low_closing_ask (i32.const -1))
    (block $done
      (loop $blocks
        (br_if $done (i32.ge_u (local.get $i) (local.get $size)))
        (local.set $k (i32.add (local.get $start) (local.get $i)))
        ;; The levels left in this block, or in the deque.
        (local.set $run (i32.sub (i32.const 56) (i32.rem_u (local.get $k) (i32.const 56))))
        (if (i32.gt_u (local.get $run) (i32.sub (local.get $size) (local.get $i)))
          (then (local.set $run (i32.sub (local.get $size) (local.get $i)))))
        (local.set $e
          (i32.add
            (i32.load (i32.add (local.get $map) (i32.shl (i32.div_u (local.get $k) (i32.const 56)) (i32.const 2))))
            (i32.mul (i32.rem_u (local.get $k) (i32.const 56)) (i32.const 72))))
        (local.set $end (i32.add (local.get $e) (i32.mul (local.get $run) (i32.const 72))))
        (local.set $slot (local.get $i))
        (loop $levels
          (local.set $bids (f64x2.add (local.get $bids) (v128.load offset=8 align=8 (local.get $e))))
          (local.set $asks (f64x2.add (local.get $asks) (v128.load offset=40 align=8 (local.get $e))))
          (local.set $bid_orders (i64x2.add (local.get $bid_orders)
            (i64x2.extend_low_i32x4_u (v128.load64_zero offset=24 align=4 (local.get $e)))))
          (local.set $ask_orders (i64x2.add (local.get $ask_orders)
            (i64x2.extend_low_i32x4_u (v128.load64_zero offset=56 align=4 (local.get $e)))))
          (local.set $q (f64.load offset=8 (local.get $e)))
          (local.set $top_bid (select (local.get $slot) (local.get $top_bid)
            (i32.or (f64.gt (local.get $q) (f64.const 0)) (f64.lt (local.get $q) (f64.const 0)))))
          (local.set $q (f64.load offset=16 (local.get $e)))
          (local.set $top_closing_bid (select (local.get $slot) (local.get $top_closing_bid)
            (i32.or (f64.gt (local.get $q) (f64.const 0)) (f64.lt (local.get $q) (f64.const 0)))))
          (local.set $q (f64.load offset=40 (local.get $e)))
          (local.set $low_ask (select (local.get $slot) (local.get $low_ask)
            (i32.and (i32.lt_s (local.get $low_ask) (i32.const 0))
              (i32.or (f64.gt (local.get $q) (f64.const 0)) (f64.lt (local.get $q) (f64.const 0))))))
          (local.set $q (f64.load offset=48 (local.get $e)))
          (local.set $low_closing_ask (select (local.get $slot) (local.get $low_closing_ask)
            (i32.and (i32.lt_s (local.get $low_closing_ask) (i32.const 0))
              (i32.or (f64.gt (local.get $q) (f64.const 0)) (f64.lt (local.get $q) (f64.const 0))))))
          (local.set $slot (i32.add (local.get $slot) (i32.const 1)))
          (local.set $e (i32.add (local.get $e) (i32.const 72)))
          (br_if $levels (i32.lt_u (local.get $e) (local.get $end))))
        (local.set $i (i32.add (local.get $i) (local.get $run)))
        (br $blocks)))
    (v128.store offset=0 align=8 (local.get $out) (local.get $bids))
    (v128.store offset=16 align=8 (local.get $out) (local.get $asks))
    (v128.store offset=32 align=8 (local.get $out) (local.get $bid_orders))
    (v128.store offset=48 align=8 (local.get $out) (local.get $ask_orders))
    (i32.store offset=64 (local.get $out) (local.get $top_bid))
    (i32.store offset=68 (local.get $out) (local.get $top_closing_bid))
    (i32.store offset=72 (local.get $out) (local.get $low_ask))
    (i32.store offset=76 (local.get $out) (local.get $low_closing_ask))))
