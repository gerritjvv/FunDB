(ns fundb.veb-disk-bench
  (:import (java.util TreeMap))
  (:require [fundb.db.index.nio.veb :refer :all])
  (:use perforate.core))

(defonce bench-counter 100000)
(defonce u-value 100000000)

(defonce id (System/currentTimeMillis))

(defn- create-veb [u]
  (reduce  (fn [v n] (v-insert! v n n)) (create-load-index (str "target/test-index-" id) u) (range u)))




(comment

  (defgoal veb-disk-bench "vEB disk benchmark"
           :setup (fn [] (let [u u-value]
                              [u (create-veb u) nil])))

  (defn call-safe [f & args]
        (try
          (apply f args)
          (catch Exception e nil)))

  (defcase veb-disk-bench :veb-insert
           [u veb & _]
           (dotimes [i bench-counter]
                    (call-safe v-insert! veb (rand-int u) 1)))


  (defcase veb-disk-bench :veb-successor
           [u veb & _]
           (dotimes [i bench-counter]
                    (call-safe v-successor veb (rand-int u))))

  (defcase veb-disk-bench  :veb-get
           [u veb & _]
           (dotimes [i bench-counter]
                    (call-safe v-get veb (long (rand-int u)))))
)
