(ns fundb.veb-utils-bench
  (:require [fundb.veb-utils :refer :all])
  (:use perforate.core))


(defgoal veb-utils-bench "Veb utils benchmark")


(defcase veb-utils-bench :high
         []
         (dotimes [i 100000]
           (high 64 (rand-int 64))))

(comment

  (defcase veb-utils-bench :low
           []
           (dotimes [i 100000]
             (low 64 (rand-int 64))))

  (defcase veb-utils-bench :index
           []
           (dotimes [i 100000]
             (index 64 (rand-int 64) (rand-int 64))))
  )