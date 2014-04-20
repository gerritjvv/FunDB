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
                                 m
                                 (assoc m db-name
                                   {:name db-name :dir (clojure.java.io/file dir) :tables {}}))))) db-name)))

(defn get-database [db-name] (get @databases db-name))

(defn create-table [db-name table-name dir]
  (if-let [table (get-in @databases [db-name :tables table-name])]
      table
      (get-in
         (dosync
            (alter databases (fn [m]
                               (if-let [table (get-in m [db-name :tables table-name])]
                                 m
                                 (assoc-in m [db-name :tables table-name]
                                           {:name table-name :dir (clojure.java.io/file dir)
                                                                   :insert-data (ref {})})))))

         [db-name :tables table-name])))
