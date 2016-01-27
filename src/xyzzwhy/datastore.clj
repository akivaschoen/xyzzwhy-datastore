(ns xyzzwhy.datastore
  (:require [clojure.string :as str]
            [environ.core :refer [env]]
            [rethinkdb.query :as r]))

(defonce db-name "xyzzwhy_corpora")

(defn test
  []
  "guch")

(defn- ->table-name
  [c]
  (-> (if (map? c)
        (:classname c)
        c)
      name
      str
      (str/replace "-" "_")))

(defn- add-fragments
  [c]
  ;;(with-open [conn (r/connect (env :xyzzwhy-corpora-db))]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/insert (:fragments c))
        (r/run conn))))

;; Existing in a pre-macro state (probably)
(defn- class-action
  [dsfn c]
  ((fn []
     (with-open [conn (r/connect)]
       (let [results (-> (r/db db-name)
                         (dsfn (->table-name c))
                         (r/run conn))]
         (println results)
         c)))))

(defn- class-query
  [dsfn]
  ((fn []
     (with-open [conn (r/connect)]
       (-> (r/db db-name)
           dsfn
           (r/run conn))))))

(defn create-database
  []
  (with-open [conn (r/connect)]
    (r/run (r/db-create db-name) conn)))

(defn add-class
  [c]
  (class-action r/table-create c))

(defn delete-class
  [c]
  (class-action r/table-drop c))

(defn empty-class
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/delete)
        (r/run conn))))

(defn get-class
  [c]
  (class-action r/table c))

(defn get-fragment
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/sample 1)
        (r/run conn)
        first)))

(defn list-classes
  []
  (class-query r/table-list))

(defn class-exists?
  [c]
  (some (partial = (->table-name c)) (list-classes)))

(def reset-class (comp add-fragments add-class delete-class))
