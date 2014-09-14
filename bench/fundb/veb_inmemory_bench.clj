(ns fundb.veb-inmemory-bench
  (:require [fundb.veb :refer :all])
  (:import [java.util TreeMap])
  (:use perforate.core))

(defonce bench-counter 1000)
(defonce u-value 1000000)

(comment

  (defn- create-veb [u]
         (reduce (fn [v n] (insert v n {:data n})) (create-root u) (range u)))

  (defn- create-tree-map [u]
         (reduce (fn [^TreeMap v n] (.put v n n) v) (TreeMap.) (range u)))


  (defgoal veb-inmemory-bench "vEB in memory benchmark - successor"
           :setup (fn [] (let [u u-value]
                              [u (create-veb u) (vector (range u)) (create-tree-map u)])))

  (defcase veb-inmemory-bench :veb-insert
           [u veb & _]
           (dotimes [i 10]
                    (create-veb u)))

  (defcase veb-inmemory-bench  :tree-map-insert
           [u _ _ ^TreeMap m]
           (dotimes [i 10]
                    (create-tree-map u)))



  (defcase veb-inmemory-bench :veb-successor
           [u veb & _]
           (dotimes [i bench-counter]
                    (successor veb (rand-int u))))

  (defcase veb-inmemory-bench  :tree-map-scuccessor
           [u _ _ ^TreeMap m]
           (dotimes [i bench-counter]
                    (.tailMap m (long (rand-int u)) false)))



  (defcase veb-inmemory-bench  :veb-get
           [u veb & _]
           (dotimes [i bench-counter]
                    (find-data veb (long (rand-int u)))))


  (defcase veb-inmemory-bench  :tree-map-get
           [u _ _ ^TreeMap m]
           (dotimes [i bench-counter]
                    (.get m (long (rand-int u)))))


  (defcase veb-inmemory-bench  :vector-get
           [u _ v & _]
           (dotimes [i bench-counter]
                    (get v (rand-int u)))))
