(ns fundb.veb
   (:require [fundb.veb-utils :refer :all]))

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
  [v] (:max) v)


 (defn member?
   "Returns true or false dependin if x is a member of v or any of its sub nodes
    Runs in O(lg lg u) time"
   [{:keys [u min max cluster] :as v} x]
   (cond
    (or (not v) (not x)) false ;check for null
    (or (= min x) (= max x)) true
    (= u 2) false
    :else (recur (cluster (high u x)) (low u x) ))) ;recur into recursive call

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
         (index x-high (successor (cluster x-high) x-low))
         (if-let [succ-cluster (successor summary x-high)]
           (index succ-cluster (veb-min (cluster succ-cluster)))
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
          (index x-high (predecessor (cluster x-high) x-low))
          (if-let [pred-cluster (predecessor summary x-high)]
            (index pred-cluster (veb-max (cluster pred-cluster)))
            (if (and min (> x min)) min nil)))))))



