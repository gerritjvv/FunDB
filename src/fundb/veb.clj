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
  "Public function to call for inserting values, returns the modified node"
  [v x]
  (let [v-min (veb-min v)]
    (if (nil? v-min)
      (empty-insert (:u v) v x)
      (if (< x (veb-min v))
        (check-max (_insert (assoc v :min x) v-min) x)
        (check-max (_insert v x) x)))))

(declare delete)

(defn- _delete-min [[{:keys [min summary cluster u] :as v} x]]
  ;(prn "_delete-min x " x)
  (if (= x min)
    (let [first-cluster (veb-min summary)
          x2 (index u first-cluster (veb-min (cluster first-cluster)))]

      ;problem here is that x2 can again equal the minimum. We have to recursively seach for the next minimum
      ;using the successor
      (if (= x2 min)
        (let [x3 (successor v x2)]
          [(assoc v :min x3) x2])
        [(assoc v :min x2) x2]))
    [v x]))

(defn- _delete-x [[{:keys [cluster u] :as v} x]]
  ;(prn "_delete-x " x)
  (let [x-high (high u x)]
    [(assoc-in v [:cluster x-high] (delete (cluster x-high) (low u x))) x]))

(defn- _delete-calc-max [{:keys [summary cluster max min u] :as v} x]
  (if (= x max)
    (let [summary-max (veb-max summary)]
      (if (or (nil? summary-max) (nil? (veb-max (cluster summary-max))))
        min
        (index u summary-max (veb-max (cluster summary-max)))))
    max))

(defn- _delete-max [[{:keys [cluster summary max u] :as v} x]]
  (prn "_delete-max x " x)
  (let [x-high (high u x)]
    (if (nil? (veb-min (cluster x-high)))
      (let [v2 (assoc v :summary (delete summary x-high))]
        (if (= (:max v2) x)
          (let [new-max (_delete-calc-max v2 x)]
           [(assoc v2 :max (if (< new-max x) x new-max)) x]) ;enter here with v2
          [v2 x]))
       (if (= x max)
         [(assoc v :max (index u x-high (veb-max (cluster x-high)))) x]
         [v x]))))




(defn delete [{:keys [min max u] :as v} x]
  ;(prn "delete min " min " max " max " x " x)
  (if (or (< x min) (> x max))
    v
    (cond
      (= min max) (assoc (dissoc v :min :max :summary) :cluster {})
      (= u 2) (let [min (if (= x 0) 1 0)]
                (assoc v :min min :max max))
      :else
      (-> [v x] _delete-min _delete-x _delete-max first))))



(defn create-tree [u inserts]
  (reduce (fn [tree i] (insert tree i)) (create-root u) inserts))


