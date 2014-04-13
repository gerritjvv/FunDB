(ns fundb.proto-veb-utils)

;generic utility functions for the algorithms


(defn high [u x] (Math/floor (/ x (Math/sqrt u))))

(defn low [u x] (mod x (Math/sqrt u)))

(defn index [u x y] (+ (* x (Math/sqrt u)) y))


