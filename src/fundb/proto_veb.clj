(ns fundb.proto-veb
  (:require [fundb.veb-utils :refer :all]))

;Introduction to algorithms
;This file describes the algorithms from 20.2 on proto-vEB structures

;veb structure is presented by a simple map as follows
; key    explanation
; u       universe size
; summary  vector of pointers to proto-vEB(sqrt(u))
; cluster  vector of sqrt(u) pointers to proto-vEB(sqrt(u)) structures
; if u == 2 a key array is present with two elements


(defn member
  "Works in O(lg lg 2) v = proto-vEB x is an element number"
  [v x]
  (if (= (:u v) 2)
    ((:array v) x);gets index x from array in v
    (member ((:cluster v) (high x)) (low x))))

(defn minimum
  "Returns the minimum value or nil if non exists"
  [{:keys [u array summary cluster]}]
  (if (= u 2)
    (cond
     (= (array 0) 1) 0
     (= (array 1) 1) 1
     :else nil)
    (let [min-cluster (minimum summary)]
      (if minimum
        (index u min-cluster (minimum (cluster min-cluster))))
      )))

(defn successor
  "Finds the successor otherwise returns nil. Runs in O(lg u lg lg u)"
  [{:keys [u array cluster summary]} x]
  (if (= u 2)
    (if (and (= x 0) (= 1 (array 1))) 1 nil)
    (if-let [offset (successor (cluster (high x)) (low x))]
      (index (high x) offset)
      (if-let [succ-cluster (successor summary (high x))]
        (index succ-cluster (minimum (cluster succ-cluster)) 0)))))

(defn insert
  "Inserts an element into the veb structure runs in O(lg u) and returns the new tree with the element changed"
  [{:keys [array u cluster summary] :as v} x]
  (if (= u 2)
    (assoc v :array (assoc array x 1))
    (let [h (high x)]
      (assoc v
        :cluster (assoc cluster h (insert (cluster h) (low x)))
        :summary (insert summary h)))))






