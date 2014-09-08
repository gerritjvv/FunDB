(ns fundb.veb-inmemory-bench
  (:require [fundb.veb :refer :all])
  (:use perforate.core))


(defn- create-veb [u]
  (reduce (fn [v n] (insert v n {:data n})) (create-root u) (range u)))

(defgoal veb-inmemory-bench "vEB in memory benchmark"
         :setup (fn [] [10000 (create-veb 10000) (vector (range 10000))]))

(defcase veb-inmemory-bench  :vector-get
         [u _ v]
         (dotimes [i 100000]
           (get v (rand-int u))))


(defcase veb-inmemory-bench  :veb-get
         [u veb _]
         (dotimes [i 100000]
           (find-data veb (rand-int u))))

