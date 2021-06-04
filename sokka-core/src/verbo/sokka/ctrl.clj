(ns ^:no-doc verbo.sokka.ctrl
  "protocol and a default implementation of a `control` that enables
  multiple threads to co-ordinate life cycle events, like
  termination (close), kill (abort) and timeout."
  (:require [clojure.core.async :as async]
            [clojure.pprint :as pp]
            [clojure.core.async.impl.protocols :refer [closed?]])
  (:refer-clojure :exclude [deref]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                       ----==| C O N T R O L |==----                        ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Control
  (as-chan [this])
  (close! [this])
  (live? [this])
  (abort! [this])
  (aborted? [this])
  (cleanup! [this]))

(prefer-method print-method clojure.lang.IPersistentMap clojure.lang.IDeref)
(prefer-method print-method java.util.Map clojure.lang.IDeref)
(prefer-method pp/simple-dispatch
  clojure.lang.IPersistentMap clojure.lang.IDeref)
(prefer-method print-method  clojure.lang.IRecord
  clojure.lang.IDeref)

(defn default-control
  [timeout-ms]
  (let [close-chan (async/chan 1)
        abort-chan (async/chan 1)
        timeout-chan (async/timeout timeout-ms)
        p (promise)
        m (async/go
            (let [[v c] (async/alts! [timeout-chan abort-chan close-chan])]
              (condp = c
                close-chan   @(deliver p {:status :closed})
                abort-chan   @(deliver p (merge v {:status :aborted}))
                timeout-chan (do
                               (deliver p {:status :aborted
                                           :err-data {:reason :timed-out}})
                               (async/close! abort-chan)))))]
    (reify
      clojure.lang.IDeref
      (deref [_] (clojure.core/deref p))

      clojure.lang.IBlockingDeref
      (deref [_ timeout-ms timeout-val]
        (clojure.core/deref p timeout-ms timeout-val))

      Control
      (close! [_]
        (when close-chan
          (async/close! close-chan)
          :closed))

      (abort! [_]
        (when abort-chan
          (async/close! abort-chan)
          :aborted))

      (cleanup! [this]
        (close! this)
        (abort! this)
        nil)

      (live? [this]
        (not (closed? m)))

      (aborted? [this]
        (closed? abort-chan))

      (as-chan [this] m))))
