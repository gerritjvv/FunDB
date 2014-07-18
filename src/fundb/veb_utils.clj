(ns fundb.veb-utils)


(defn- perfect-square? [x]
  (let [root (Math/floor (Math/pow x 0.5))]
    (= (* root root) x)))

(defn fib? [x]
  (or (perfect-square? (+ (* x x 5) 4)) (perfect-square? (- (* x x 5) 4))))


;constants for log 2 calculations
(defonce log2 (Math/log 2))
;the limit of u at which log2-int can be used, any value larger than this should use log2-long
(defonce log-int-limit (Math/pow 2 30))

(defn ^long log2-long [^long bits]
  (/ (Math/log bits) log2))

(defn ^long log2-int
  "Fast log2 calculation for integer numbers 2^x =u   log(u)=x
   http://stackoverflow.com/questions/3305059/how-do-you-calculate-log-base-2-in-java-for-integers
   user: http://stackoverflow.com/users/237321/x4u"
  [^long bits]
  (if (= bits 0) 0 (- 31 (Integer/numberOfLeadingZeros (int bits)))))

(defn veb-sqrt
  "Calculates the sqrt based on 2^([lg u]/2) where lg is base 2"
  [^long u]
  ;we use the fast int function if the u is smalle enough, otherwise use the slower long function
  (Math/pow 2 (/ (if (> u log-int-limit) (log2-long u) (log2-long u)) 2)))

(defn lower-sqrt [u]
  (long (Math/floor (veb-sqrt u))))

(defn upper-sqrt [u]
  (long (Math/ceil (veb-sqrt u))))

(defn high
  "Calculates the high value of x taking into account u as a power of 2 where sqrt(u) is not an exact number"
  [u x]
  (long (Math/floor (/ x (lower-sqrt u)))))

(defn low
  "Calculates the low value of x taking into account u as a power of 2 where sqrt(u) is not an exact number"
  [u x]
  (long (mod x (lower-sqrt u))))

(defn index
  "Calculates the index value of x taking into account u as a power of 2 where sqrt(u) is not an exact number"
  [u x y]
  (+ (* x (lower-sqrt u)) y))

