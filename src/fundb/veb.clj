(ns fundb.veb
  (:require [fundb.veb-utils :refer :all])
  (:use [clojure.core :exclude [min max]]))

;
;The vEB tree structure is represented by nodes that each are represented by a map.
;The keys for the map are:
;u ;the universe size
;mini the minimum
;max the maximum
;summary the summary vector pointing to vEB node structures
;cluster the cluster vector pointing to vEB node structures
;if u is 2 then this is a base case and the keys summary and cluster are not represent


(defn veb-min
  "Returns the minimum for v and runs in constant time"
  [v] (:min v))

(defn veb-max
  "Returns the maximum for v and runs in constant time"
  [v] (:max v))


 (defn member?
   "Returns true or false dependin if x is a member of v or any of its sub nodes
    Runs in O(lg lg u) time"
   [{:keys [u min max cluster] :as v} x]
   (if u
     (cond
      (or (not v) (not x)) false ;check for null
      (or (= min x) (= max x)) true
      (= u 2) false
      :else (recur (cluster (high u x)) (low u x) )) ;recur into recursive call
     false))

 (defn successor
   "Finds the successor of x in v and its subnodes
    Runs in O(lg lg u) time"
   [{:keys [u max min cluster summary] :as v} x]
   (if (or  (= u 2) (and cluster summary))
     (cond
      (= u 2) (if (and (= 0 x) (= 1 max)) 1 nil)
      (and min (< x min)) min
      :else
      (let [x-high (high u x)
            max-low (veb-max (cluster x-high))
            x-low (low u x)]
        (if (and max-low (< x-low max-low))
         (index u x-high (successor (cluster x-high) x-low))
         (if-let [succ-cluster (successor summary x-high)]
           (index u succ-cluster (veb-min (cluster succ-cluster)))
           nil))))))

 (defn predecessor
   "Finds the predecessor of x in v and its subnodes
    Runs in O(lg lg u) time"
   [{:keys [u min max cluster summary]} x]
   (if (or  (= u 2) (and cluster summary))
     (cond
      (= u 2) (if (and (= 1 x) (= min 0)) 0 nil)
      (and max (> x max)) max
      :else
      (let [x-high (high u x)
            min-low (veb-min (cluster x-high))
            x-low (low u x)]
        (if (and min-low (> x-low min-low))
          (index u x-high (predecessor (cluster x-high) x-low))
          (if-let [pred-cluster (predecessor summary x-high)]
            (index u pred-cluster (veb-max (cluster pred-cluster)))
            (if (and min (> x min)) min nil)))))))


(declare insert)


(defn create-root
  "Create a root node"
  [u]
  {:u u :cluster {}})

(defn- empty-insert
  "Handles the different cases for inserting an empty node and creates to the currect attributes based on u"
  [u v x]
  (if (:u v)
    (assoc v :min x :max x)
    (let [u-root (upper-sqrt u)]
      (if (> u-root 2)
        {:u u-root :min x :max x :summary {:u (upper-sqrt u-root) :cluster {}} :cluster {}}
        {:u u-root :min x :max x :cluster {}}))))


(defn- check-max
  "If the max is nil both the min and max values are set to x-low
   if x is bigger than max x is set to max, otherwise v is returend as is"
  [v x]
  (let [v-max (:max v)]
    (if (nil? v-max)
      (assoc v :max x :min x)
      (if (> x v-max)
        (assoc v :max x) v))))

(defn- get-summary
  "Handles nil summary cases if the summary is nil a map of {:u (upper-sqrt u) :cluster {}} is returned
   otherwise the summary is returned as is"
  [u summary]
  (if summary summary {:u (upper-sqrt u) :cluster {}}))


(defn- _insert
  "Helper function to insert, only inserts if u is bigger than 2 and returns the new modified tree"
  [{:keys [u cluster summary] :as v} x]
  (if (> u 2)
    (let [x-high (high u x)
          x-low (low u x)]
      (if (nil? (veb-min (cluster x-high)))
        (assoc v :summary (insert (get-summary u summary) x-high)
          :cluster (assoc cluster x-high (empty-insert u (cluster x-high) x-low)))
        (assoc-in v [:cluster x-high] (insert (cluster x-high) x-low))))
    v))


(defn insert
  "Public function to call for inserting values, returns the modified node
   Runs in O(lg lg u)"
  [v x]
  (if (not (member? v x))
    (let [v-min (veb-min v)]
      (if (nil? v-min)
        (empty-insert (:u v) v x)
        (if (< x (veb-min v))
          (check-max (_insert (assoc v :min x) v-min) x)
          (check-max (_insert v x) x))))
    v))

(declare delete)

(defn- _delete-min
  "Called by delete and sets the min value"
  [[{:keys [min summary cluster min max u] :as v} x]]
  (if (and (not (empty? cluster)) (= x min) (veb-min (cluster (veb-min summary))))
    (let [first-cluster (veb-min summary)
          x2 (index u first-cluster (veb-min (cluster first-cluster)))]
      [(assoc v :min x2) x2])
    [v x]))

(defn- _delete-clean-node [{:keys [min max] :as v}]
  (if (and (nil? min) (nil? max))
    nil
    v))

(defn- _delete-x
  "Called by delete and deletes x from v"
  [[{:keys [cluster min max u] :as v} x]]
  (let [x-high (high u x)
        node (_delete-clean-node (delete (cluster x-high) (low u x)))
        v2 (if node
             (assoc-in v [:cluster x-high] node) ;remove empty nodes
             (assoc v :cluster (dissoc cluster x-high)))]
    [v2 x]))


(defn- _delete-max
  "Called by delete and sets the max value"
  [[{:keys [cluster summary min max u] :as v} x]]
  (if (and cluster (nil? (veb-min (cluster (high u x)))))
    (let [summary2 (_delete-clean-node (delete summary (high u x)))
          v2 (assoc v :summary summary2)]
      (if (= x max)
        (let [summary-max (veb-max summary2)]
          (if (or (nil? summary-max) (nil? (veb-max (cluster summary-max))))
            [(assoc v2 :max min) x]
            [(assoc v2 :max (index u summary-max (veb-max (cluster summary-max)))) x]))
        [v2 x]))
    (if (= x max)
      (let [x-high (high u x)]
        [(assoc v :max (index u x-high (veb-max (cluster x-high)))) x])
      [v x])))



(defn delete
  "Delete the member x from the tree v
   Runs in O(lg lg u) time"
  [{:keys [min max u] :as v} x]

  (if (or (nil? u) (nil? min) (nil? max) (< x min))
    v
    (cond
      (= (:min v) (:max v))
      (assoc (dissoc v :min :max :summary) :cluster {})
      (= u 2) (let [min (if (= x 0) 1 0)]
                (assoc v :min min :max min))
      :else
      (-> [v x] _delete-min _delete-x _delete-max first))))



(defn create-tree
  "Helper function to create a tree and applying n inserts i.e each element in inserts is inserted into a tree of size u"
  [u inserts]
  (reduce (fn [tree i] (insert tree i)) (create-root u) inserts))



