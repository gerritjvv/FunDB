(ns fundb.veb-utils)

;constants for log 2 calculations
(defonce ^Double log2 (Math/log 2))
;the limit of u at which log2-int can be used, any value larger than this should use log2-long
(defonce ^Long log-int-limit (Math/pow 2 30))

(defn ^double log2-long [^long bits]
  (/ (Math/log bits) log2))

(defn ^double log2-int
  "Fast log2 calculation for integer numbers 2^x =u   log(u)=x
   http://stackoverflow.com/questions/3305059/how-do-you-calculate-log-base-2-in-java-for-integers
   user: http://stackoverflow.com/users/237321/x4u"
  [^long bits]
  (if (= bits 0) 0 (- 31 (Integer/numberOfLeadingZeros (int bits)))))

(defn ^long veb-sqrt
  "Calculates the sqrt based on 2^([lg u]/2) where lg is base 2"
  [^long u]
  ;we use the fast int function if the u is smalle enough, otherwise use the slower long function
  (Math/pow 2 (/ (if (> u log-int-limit) (log2-long u) (log2-long u)) 2)))

(defn ^long lower-sqrt [^long u]
  (long (Math/floor (veb-sqrt u))))

(defn ^long upper-sqrt [^long u]
  (long (Math/ceil (veb-sqrt u))))

(defn ^long high
  "Calculates the high value of x taking into account u as a power of 2 where sqrt(u) is not an exact number"
  [^long u ^long x]
  ;casting to long performs an implicit floor function
  (long (/ x ^long (upper-sqrt u))))

(defn ^long low
  "Calculates the low value of x taking into account u as a power of 2 where sqrt(u) is not an exact number"
  [^long u ^long x]
  (long (mod x (upper-sqrt u))))

(defn ^long index
  "Calculates the index value of x taking into account u as a power of 2 where sqrt(u) is not an exact number"
  [^long u ^long x ^long y]
  (+ (* x (upper-sqrt u)) y))

(defn max-number-of-nodes
  "Calculates the max number of nodes a tree will create"
  [^long u]
  (let [^long x (upper-sqrt u)]
    (if (> x 2) (+ x (* x (max-number-of-nodes x)))
                2)))

