(ns verbo.sokka.test-helpers
  (:require [midje.sweet :as midje]
            [midje.checking.core :as checking]
            [schema.core :as schema]))

(defn error=
  ([t]
   (error= t nil))

  ([t e]
   (fn [exception]
     (when-let [data (ex-data exception)]
       (cond-> (= (:type data) t)
         e (and (= (:error data) e)))))))

(def bad-request? (partial error= :bad-request))

(def forbidden? (partial error= :forbidden))

(midje/defchecker conforms?
  [s]
  (midje/checker [actual]
    (if-let [error (schema/check s actual)]
      (checking/as-data-laden-falsehood
        {:notes [(if (instance? schema.utils.ValidationError error)
                   (schema.utils/validation-error-explain error)
                   error)]})
      actual)))
