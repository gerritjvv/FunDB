(ns fundb.storage)


(def databases (ref {}))


(defn create-db
  "If the database already exists the current db is returned otherwise it is created and returned"
  [db-name dir]
     (if-let [stored-db (get @databases db-name)]
       stored-db
       (get (dosync
             (alter databases (fn [m]
                               (if-let [stored-db (get m db-name)]
                                 stored-db
                                 (assoc m db-name
                                   {:name db-name :dir (clojure.java.io/file dir) :tables {}}))))) db-name)))


