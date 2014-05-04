(ns fundb.veb
  (:require [fundb.veb-utils :refer :all])
  (:use [clojure.core :exclude [min max]]))

;IMPORTANT: only works for u values 2^4, 2^24 and 2^32
;The vEB tree structure is represented by nodes that each are represented by a map.
;The keys for the map are:
;u ;the universe size
;min a map with the minimum stored as :v
;max a map with the maximum stored as :v
;summary the summary vector pointing to vEB node structures
;cluster the cluster vector pointing to vEB node structures
;if u is 2 then this is a base case and the keys summary and cluster are not represent


(defn veb-min
  "Returns the minimum for v and runs in constant time"
  [v] (:v (:min v)))

(defn veb-max
  "Returns the maximum for v and runs in constant time"
  [v] (:v (:max v)))


 (defn member?
   "Returns true or false dependin if x is a member of v or any of its sub nodes
    Runs in O(lg lg u) time"
   [{:keys [u min max cluster] :as v} x]
   (if u
     (cond
      (or (not v) (not x)) false ;check for null
      (or (= (:v min) x) (= (:v max) x)) true
      (= u 2) false
      :else (recur (cluster (high u x)) (low u x) )) ;recur into recursive call
     false))

 (defn successor
   "Finds the successor of x in v and its subnodes
    Runs in O(lg lg u) time"
   [{:keys [u max min cluster summary] :as v} x]
   ;(prn "u " u " x " x " x-hig " (high u x) " low " (low u x) " v " v)
   (cond
         (= u 2)
         (if (and (= x 0) (= (:v max) 1)) [1 max] nil)
         (and (:v min) (< x (:v min)))
         [(:v min) min]
         :else
         (let [x-high (high u x)
               x-low (low u x)
               max-low (veb-max (get cluster x-high))]
           (if (and max-low (< x-low max-low))
             (let [[succ-i data2] (successor (cluster x-high) x-low)]
               ;(prn "x-high " x-high  " x " x " succ-i " succ-i   " u " u " x-high " x-high " v " v)
               [(index u x-high succ-i) data2])
             (let [[succ-cluster data] (do  (prn "summary " summary " x-high " x-high " x " x)
                                  (successor summary x-high))]
               (if (not succ-cluster)
                 nil
                 [(index u succ-cluster (veb-min (cluster succ-cluster))) data]  ))))))


 (defn successor-v [v x]
   (first (successor v x)))

 (defn veb-data
   "Returns the data associated with x otherwise nil"
   [v x]
   (if-let [succ-data (successor v (dec x))]
     (let [[succ-i data] succ-data]
       (if (= succ-i x)
         data))))

 (defn predecessor
   "Finds the predecessor of x in v and its subnodes
    Runs in O(lg lg u) time"
   [{:keys [u min max cluster summary] :as v} x]

   (if (or  (= u 2) (and cluster summary))
     (cond
      (= u 2) (if (and (= 1 x) (= (:v min) 0)) 0 nil)
      (and (:v max) (> x (:v max))) (:v max)
      :else
      (let [x-high (high u x)
            min-low (veb-min (cluster x-high))
            x-low (low u x)]
        (if (and min-low (> x-low min-low))
          (index u x-high (predecessor (cluster x-high) x-low))
          (if-let [pred-cluster (predecessor summary x-high)]
            (index u pred-cluster (veb-max (cluster pred-cluster)))
            (if (and (:v min) (> x (:v min))) (:v min) nil)))))))

(defn _veb-tree-seq [v start-x]
  (lazy-seq
          (if-let [succ (successor v start-x)]
            (cons succ (_veb-tree-seq v (first succ))))))

(defn veb-tree-seq
  ([v] (veb-tree-seq v (veb-min v)))
  ([v start-x]
   (if (member? v start-x)
     (cons (successor v (dec start-x)) (_veb-tree-seq v start-x))
     (_veb-tree-seq v start-x))))



(declare insert)


(defn create-root
  "Create a root node"
  [u]
  {:u u :cluster {}})

(defn- empty-insert
  "Handles the different cases for inserting an empty node and creates to the currect attributes based on u"
  [u v x data]
  (if (:u v)
    (assoc v :min (assoc data :v x) :max (assoc data :v x) )
    (let [u-root (upper-sqrt u)
          data-m (assoc data :v x)]
      (if (> u-root 2)
        {:u u-root :min data-m :max data-m :summary {:u (upper-sqrt u-root) :cluster {}} :cluster {}}
        {:u u-root :min data-m :max data-m}))))

(defn- add-to-cluster [v x cx]
  (assoc-in v [:cluster x] cx))

(defn- get-summary [v]
  (if-let [s (:summary v)]
    s
    {:u (upper-sqrt (:u v)) :min nil :max nil}))

(defn- add-summary [v x data]
  ;(prn "add summary" (get-summary v)  " x " x)
  (assoc v :summary (insert (get-summary v) x data)))

(defn- check-max [{:keys [max] :as v} x data]
  (if (> x (:v max))
    (assoc v :max (assoc data :v x))
    v))


(defn- get-cluster [v x data]
  (if-let [c (get-in v [:cluster x])]
    c
    {:u (upper-sqrt (:u v)) :min nil :max :nil}))

(defn- exhange-x-with-min [{:keys [min] :as v} x data]
  (let [v2 (assoc v :min (assoc data :v x))]
    (insert v2 (:v min) (dissoc min :v))))

(defn insert [{:keys [u min] :as v} x data]
  ;(prn "insert :u " u " x " (veb-min v))
  (if (veb-min v)
    ;put in here if x < v.min exhange x with v.min
    (cond
     (< x (:v min))
     (exhange-x-with-min v x data)
     (> u 2)
     (let [x-high (high u x)
            x-low (low u x)
            c (get-cluster v x-high data)]
        (if (veb-min (get-in v [:cluster x-high]))
          (->
           v
           (add-to-cluster x-high (insert c x-low data))
           (check-max x data))
          (->
           v
           (add-summary x-high data)
           (add-to-cluster x-high (insert c x-low data))
           (check-max x data))))
      :else (check-max v x data))
    (empty-insert u v x data)))


(defn create-tree
  "Helper function to create a tree and applying n inserts i.e each element in inserts is inserted into a tree of size u"
  [u inserts data]
  (reduce (fn [tree i] (insert tree i data)) (create-root u) inserts))



