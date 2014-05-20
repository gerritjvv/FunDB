(ns fundb.hashes)

;order preserving hashing is important for storing keys in vEB trees.
;if the order is not preserved to keys inserted will not be in order and
;will be randomly distributed over the vEB tree.
;
;The java method .hashCode based on s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
;is order preserving.

(defprotocol OrderPreservingHash
  (o-hash [x] "Returns a hash that is order preserving"))


(extend-type java.lang.String

  OrderPreservingHash
  (o-hash [x] (.hashCode ^String x)))

(extend-type java.lang.Long

  OrderPreservingHash
  (o-hash [x] (.hashCode ^Long x)))


(extend-type java.lang.Integer

  OrderPreservingHash
  (o-hash [x] (.hashCode ^Integer x)))

