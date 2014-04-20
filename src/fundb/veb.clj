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

(defn delete-base-case [v x]
  (let [m (if (= x 0) 1 0)]
    (assoc v :min m :max m)))

(defn delete [v x]
  (if (:u v)
    (cond
     (= (:min v) (:max v))
     (assoc v :min nil :max nil)
     (= (:u v) 2)
     (delete-base-case v x)
     :else
     (let [min2 (if (and (= x (:min v)) (veb-min ((:cluster v) (veb-min (:summary v)))))
                 (let [first-cluster (veb-min (:summary v))
                     x (index (:u v) first-cluster (veb-min ((:cluster v) first-cluster)))]
                   x)
                 (:min v))
           v2 (delete ((:cluster v) (high (:u v) x)) (low (:u v) x))
           ]
       (if v2
         (let [
           [summary2 max2](if (nil? (veb-min ((:cluster v2) (high (:u v2) x))))
                           (let [summary3 (delete (:summary v2) (high (:u v2) x))]
                             (if (= x (:max v2))
                               (let [summary-max (veb-max summary3)]
                                 (if (nil? summary-max)
                                   [summary3 (:min v2)]
                                   [summary3 (index (:u v2) summary-max (veb-max ((:cluster v2) summary-max)))]))
                               [(:summary v2) (:max v2)]))
                            (if (= x (:max v2))
                              [(:summary v2) (index (:u v2) (high (:u v2) x) (veb-max ((:cluster v2) (high (:u v2) x))))]
                              [(:summary v2) (:max v2)]))]
           (assoc v2 :min min2 :max max2 :summary summary2))
         nil)))))




(defn create-tree
  "Helper function to create a tree and applying n inserts i.e each element in inserts is inserted into a tree of size u"
  [u inserts]
  (reduce (fn [tree i] (insert tree i)) (create-root u) inserts))



