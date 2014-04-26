(ns fundb.veb
  (:require [fundb.veb-utils :refer :all])
  (:use [clojure.core :exclude [min max]]))

;
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
   (if true
     (cond
      (= u 2) (if (and (= 0 x) (= 1 (:v max))) [1 max] nil)
      (and min (< x (:v min))) [(:v min) min]
      :else
      (let [x-high (high u x)
            max-low (veb-max (cluster x-high))
            x-low (low u x)]
        (if (and max-low (< x-low max-low))
         (let [[succ-index data] (successor (cluster x-high) x-low)]
           (if (not succ-index)
             (do (prn "NIL HERE see what predecessor does min: " min " x " x " succ-summ " (successor summary x-high)) nil)
            [(index u x-high succ-index) data]))
         (if-let [[succ-cluster-i succ-data] (successor summary x-high)]
          (do  (prn "succ-cluster-i " succ-cluster-i " cluster " cluster)
           [ (index u succ-cluster-i (veb-min (cluster succ-cluster-i))) succ-data]
           nil)))))))


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
        {:u u-root :min data-m :max data-m :cluster {}}))))


(defn- check-max
  "If the max is nil both the min and max values are set to x-low
   if x is bigger than max x is set to max, otherwise v is returend as is"
  [v x data]
  (let [v-max (:max v)
        max-d (assoc data :v x)]
    (if (nil? (:v v-max))
      (assoc v :max max-d :min max-d)
      (if (> x (:v v-max))
        (assoc v :max max-d) v))))

(defn- get-summary
  "Handles nil summary cases if the summary is nil a map of {:u (upper-sqrt u) :cluster {}} is returned
   otherwise the summary is returned as is"
  [u summary]
  (if summary summary {:u (upper-sqrt u) :cluster {}}))


(defn- _insert
  "Helper function to insert, only inserts if u is bigger than 2 and returns the new modified tree"
  [{:keys [u cluster summary] :as v} x data]
  (if (> u 2)
    (let [x-high (high u x)
          x-low (low u x)]
      (if (nil? (veb-min (cluster x-high)))
        (assoc v :summary (insert (get-summary u summary) x-high data)
          :cluster (assoc cluster x-high (empty-insert u (cluster x-high) x-low data)))
        (assoc-in v [:cluster x-high] (insert (cluster x-high) x-low data))))
    v))


(defn insert
  "Public function to call for inserting values, returns the modified node
   Runs in O(lg lg u)

  data must be a map"
  [v x data]
  (if (not (member? v x))
    (let [v-min (veb-min v)]
      (if (nil? v-min)
        (empty-insert (:u v) v x data)
        (if (< x v-min)
          ;we swap x and data with the minimum, and insert data = (:min v) v = v-min
          (check-max (_insert (assoc v :min (assoc data :v x)) v-min (:min v)) x data)
          (check-max (_insert v x data) x data))))
    v))



(defn create-tree
  "Helper function to create a tree and applying n inserts i.e each element in inserts is inserted into a tree of size u"
  [u inserts data]
  (reduce (fn [tree i] (insert tree i data)) (create-root u) inserts))



