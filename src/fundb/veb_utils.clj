(ns fundb.veb-utils
  (:import [fundb.utils MathUtils]))



(defn ^long log2-int
  "Fast log2 calculation for integer numbers
   http://stackoverflow.com/questions/3305059/how-do-you-calculate-log-base-2-in-java-for-integers
   user: http://stackoverflow.com/users/237321/x4u"
  [^long bits]
  (if (= bits 0) 0  (- 31 (Integer/numberOfLeadingZeros (int bits)))))

(defn ^double veb-sqrt
  "Calculates the sqrt based on 2^([lg u]/2) where lg is base 2"
  [^long u]
  (Math/pow 2 (/ (log2-int u) 2)))

(defn ^double lower-sqrt [u]
   (Math/floor (veb-sqrt u)))

(defn ^double upper-sqrt [u]
   (Math/ceil (veb-sqrt u)))

(defn high
  "Calculates the high value of x taking into account u as a power of 2 where sqrt(u) is not an exact number"
  [u x]
  (Math/floor (/ x (lower-sqrt u))))

(defn low
  "Calculates the low value of x taking into account u as a power of 2 where sqrt(u) is not an exact number"
  [u x]
  (mod x (lower-sqrt u)))

(defn index
  "Calculates the index value of x taking into account u as a power of 2 where sqrt(u) is not an exact number"
  [u x y]
  (+ (* x (lower-sqrt u)) y))
