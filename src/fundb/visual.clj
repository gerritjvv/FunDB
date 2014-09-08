(ns fundb.visual
  (:use rhizome.viz))


(comment

  (def g
    {:a [:b :c]
     :b [:c]
     :c []})

  (view-graph (keys g) g
              :node->descriptor (fn [n] {:label n}))

  )

(defn )
