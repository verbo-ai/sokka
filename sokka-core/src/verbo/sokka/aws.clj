;; credit - adopted from https://github.com/BrunoBonacci/1config/
(ns verbo.sokka.aws
  (:require
   [cognitect.aws.client.api  :as aws]
   [cognitect.aws.http.cognitect]
   [cognitect.aws.credentials :as credentials]))

(defn invoke
  "Invokes an AWS request and returns the result.
  Errors are contained within the result"
  [client operation request]
  (aws/invoke client {:op      operation
                      :request request}))

(defn anomaly?
  "If an anomaly category is detected there has been an error."
  [response]
  (:cognitect.anomalies/category response))

(defn invoke!
  "Wraps `invoke` and will throw any errors as exceptions"
  [client operation request]
  (let [resp (invoke client operation request)]
    (when (anomaly? resp)
      (throw
        (ex-info
          (str "ERROR - executing " operation " - "
            (or
              (:cognitect.anomalies/message resp)
              (-> resp :Error :Message)
              (:Message resp)
              (:message resp)
              (:__type  resp)))
          resp)))
    resp))

(def shared-http-client
  ;; required for graalvm
  (delay (cognitect.aws.http.cognitect/create)))

(defn create-client
  "Creates an AWS client for the specified api."
  [{:keys [api region auth endpoint-override]}]
  (aws/client
    (-> {:api api
         :http-client @shared-http-client}
      (merge (when region {:region region}))
      (merge (when auth {:credentials-provider auth}))
      (merge (when endpoint-override
               {:endpoint-override endpoint-override})))))

(def make-client
  "Creates a client for the specified api with the given configuration"
  (memoize
    (fn [{:keys [region endpoint-override] :as cfg} api]
      (create-client
        {:api               api
         :region            region
         :auth              nil ;; default chain
         :endpoint-override (or (:all endpoint-override)
                              (api endpoint-override))}))))

(defn help
  "Will return options of what the aws api can do.

  `[client]` - Will list possible operations
  `[client op]` - Will list detail about the operation"
  ([client]
   (->> (aws/ops client)
     (map first)
     sort))
  ([client op]
   (aws/doc client op)))

(defn default-cfg
  "returns aws default config. TODO: future use"
  []
  {})

(defn get-caller-identity
  []
  (invoke! (make-client (default-cfg) :sts) :GetCallerIdentity {}))
