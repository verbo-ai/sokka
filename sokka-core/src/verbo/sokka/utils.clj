(ns ^:no-doc verbo.sokka.utils
  (:require [taoensso.nippy :as nippy]
            [taoensso.nippy.compression :as compression])
  (:import [java.util Arrays UUID Base64]
           [java.nio ByteBuffer HeapByteBuffer]
           java.util.UUID
           [java.io DataInputStream]))

(defn now
  []
  (System/currentTimeMillis))

(defn deep-merge
  "Like merge, but merges maps recursively."
  [& maps]
  (let [maps (filter (comp not nil?) maps)]
    (if (every? map? maps)
      (apply merge-with deep-merge maps)
      (last maps))))

(defn rand-id
  "Returns a 128 bit random id (based on UUID) in a short format.
   It generates a random UUID and then encodes it into base36."
  []
  (let [^UUID       id (UUID/randomUUID)
        ^ByteBuffer buf (ByteBuffer/allocate (* 2 Long/BYTES))
        _ (.putLong buf (.getMostSignificantBits id))
        _ (.putLong buf (.getLeastSignificantBits id))]
    (-> (java.math.BigInteger. 1 (.array buf))
      (.toString Character/MAX_RADIX))))

(defn ->bytes
  [^String s]
  (when s
    (.getBytes s)))

(defn bytes->base64
  [b]
  (when b
    (.encodeToString (Base64/getEncoder) b)))

(defn base64->bytes
  [^String to-decode]
  (when (seq to-decode)
    (.decode (Base64/getDecoder) to-decode)))

(defn lazy-mapcat
  "maps a function over a collection and
   lazily concatenate all the results."
  [f coll]
  (lazy-seq
    (if (not-empty coll)
      (concat
        (f (first coll))
        (lazy-mapcat f (rest coll))))))

;; lazy wrapper for query
(defn lazy-query
  "Takes a query as a lambda function and retunrs
   a lazy pagination over the items"
  ([q]
   ;; mapcat is not lazy so defining one
   (lazy-mapcat :items (lazy-query q nil)))
  ;; paginate lazily the query
  ([q start-from]
   (let [result (q start-from)]
     (lazy-seq
       (if-let [next-page (:last-evaluated-key result)]
         (cons result
           (lazy-query q next-page))
         [result])))))

(defn query-results->paginated-response
  "transform result of a dynamodb query to a paginated response with
  keys :cursor and :data. Takes an optional transformation function
  that will be applied to all items in the result."
  ([result]
   (query-results->paginated-response identity result))
  ([tf {:keys [LastEvaluatedKey Items] :as e}]
   (cond-> {:data (mapv tf Items)}
     LastEvaluatedKey (assoc :cursor {:last-evaluated-key LastEvaluatedKey}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                                                            ;;
;;   ----==| R A W   S E R I A L I Z A T I O N   F U N C T I O N S |==----    ;;
;;                                                                            ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ^:const DEFAULT_NIPPY_CONFIG
  {;; The compressor used to compress the binary data
   ;; possible options are: :none, :lz4, snappy, :lzma2,
   ;; :lz4hc. Default :lz4
   :compressor :lz4

   ;; The encryption algorithm used to encrypt the payload
   ;; :encryptor aes128-encryptor

   ;; If you wish to encrypt the data then add a password.
   ;; :password [:salted "your-pass"]

   ;; If you wish track how much time the serialization
   ;; takes and how big is the payload you need to
   ;; provide a `:metrics-prefix` value
   ;; :metrics-prefix ["kv" "binary"]
   })

(def ->compressor
  #(case %
     :lz4     compression/lz4-compressor
     :snappy  compression/snappy-compressor
     :lzma2   compression/lzma2-compressor
     :lz4hc   compression/lz4hc-compressor
     :none    nil
     :default nil))

(defn- ->nippy-config
  [config]
  (-> (merge DEFAULT_NIPPY_CONFIG config)
    (update :compressor ->compressor)))

(defn serialize
  ([val]
   (serialize val DEFAULT_NIPPY_CONFIG))

  ([val config]
   (nippy/freeze val (->nippy-config config))))

(defn bytes->byte-buffer
  [bs]
  (ByteBuffer/wrap bs))

(def ^:const byte-array-type (type (byte-array 1)))

(defn byte-array? [val]
  (= (type val) byte-array-type))

(defn byte-buffer? [val]
  (= (type val) java.nio.HeapByteBuffer))

(defn buffered-input-stream? [val]
  (= (type val) java.io.BufferedInputStream))

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [is (clojure.java.io/input-stream x)
              out (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy is out)
    (.toByteArray out)))

(defn deserialize
  ([val]
   (deserialize val DEFAULT_NIPPY_CONFIG))

  ([val config]
   (let [config (->nippy-config config)]
     (cond
       (byte-buffer? val)
       (nippy/thaw (.array ^java.nio.HeapByteBuffer val) config)

       (buffered-input-stream? val)
       (nippy/thaw (slurp-bytes val) config)

       (byte-array? val)  (nippy/thaw val config)
       :else val))))

(defn scroll
  "Wraps the query function `qfn` supplied in a lazy sequence. `qfn`
  must accept one argument `cursor` and return a map containing
  keys :data and :cursor (next cursor). :data must be a collection. If
  there are no more items to return, the :cursor must be nil."
  ([qfn]
   (scroll qfn {}))

  ([qfn {:keys [limit next-page] :as cursor}]
   (let [{data :data next-cursor :cursor} (qfn cursor)]
     (into data
       (if next-cursor
         (lazy-seq (scroll qfn next-cursor))
         nil)))))

(defmacro defalias
  "Create a local var with the same value of a var from another namespace"
  [dest src]
  `(do
     (def ~dest (var ~src))
     (alter-meta! (var ~dest) merge (select-keys (meta (var ~src)) [:doc :arglists]))))
