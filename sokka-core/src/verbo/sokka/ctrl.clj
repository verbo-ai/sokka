(ns verbo.sokka.ctrl
  (:require [clojure.core.async :as async]
            [clojure.pprint :as pp])
  (:refer-clojure :exclude [deref]))

(def ^:const DEFAULT-TASK-TIMEOUT (* 10 60 1000))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;                       ----==| C O N T R O L |==----                        ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defprotocol Control
  (abort! [this])
  (close! [this])
  (cleanup! [this]))

(defrecord DefaultControl [close-chan abort-chan timeout-chan p]
  clojure.lang.IDeref
  (deref [_] (clojure.core/deref p))

  clojure.lang.IBlockingDeref
  (deref [_ timeout-ms timeout-val]
    (clojure.core/deref p timeout-ms timeout-val))

  Control
  (close! [_] (when close-chan
                (async/close! close-chan)
                :closed))
  (abort! [_] (when abort-chan
                (async/close! abort-chan)
                :aborted))
  (cleanup! [this]
    (close! this)
    (abort! this)
    nil))

(prefer-method print-method clojure.lang.IPersistentMap clojure.lang.IDeref)
(prefer-method print-method java.util.Map clojure.lang.IDeref)
(prefer-method pp/simple-dispatch
  clojure.lang.IPersistentMap clojure.lang.IDeref)
(prefer-method print-method  clojure.lang.IRecord
  clojure.lang.IDeref)

(defn new-control
  ([] (new-control DEFAULT-TASK-TIMEOUT))

  ([timeout-ms]
   (let [close-chan (async/chan 1)
         abort-chan (async/chan 1)
         timeout-chan (async/timeout timeout-ms)]
     (->DefaultControl
       close-chan
       abort-chan
       timeout-chan
       (promise)))))
