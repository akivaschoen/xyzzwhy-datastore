(ns xyzzwhy.datastore
  (:require [clojure.string :as str]
            [environ.core :refer [env]]
            [rethinkdb.query :as r]))

(defonce db-name "xyzzwhy_corpora")

(defn- ->table-name
  [c]
  (let [n (-> (if (map? c)
                (:classname c)
                c)
              name
              str
              (str/replace "-" "_"))]
    (if (str/ends-with? n "s")
      n
      (str n "s"))))

(declare class-action)
(defn- create-class
  [c]
  (class-action r/table-create c)
  c)

(defn- add-metadata
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        (r/insert {:name (->table-name (:classname c))
                   :config (:config c)
                   :type (:type c)})
        (r/run conn)))
  c)

(defn- add-fragments
  [c]
  ;;(with-open [conn (r/connect (env :xyzzwhy-corpora-db))]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/insert (:fragment c))
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

(defn- create-database
  []
  (with-open [conn (r/connect)]
    (r/run (r/db-create db-name) conn)))

(declare get-class)
(defn- delete-class
  [c]
  (let [c (if (string? c)
            (get-class (->table-name c))
            c)]
    (class-action r/table-drop c)
    c))

(defn- delete-fragments
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/delete)
        (r/run conn))))

(defn- delete-metadata
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        (r/filter (r/fn [table]
                    (r/eq (->table-name c) (r/get-field table :name))))
        (r/delete)
        (r/run conn))))

(defn- initialize-database
  []
  (with-open [conn (r/connect)]
    (class-action r/table-create "classes")
    (-> (r/db db-name)
        (r/table "classes")
        (r/index-create "name" (r/fn [row]
                                 (r/get-field row :classname)))
        (r/run conn))
    (-> (r/db db-name)
        (r/table "classes")
        (r/index-create "type" (r/fn [row]
                                 (r/get-field row :type)))
        (r/run conn))))

(defn- cast-values
  "Returns a map with its values converted to keywords as necessary."
  [m]
  (reduce (fn [acc item]
            (assoc acc (first item)
                   (cond
                     (= :text (first item)) (second item)
                     (string? (second item)) (keyword (second item))
                     (and (vector? (second item))
                          (not= :prep (first item))) (mapv keyword (second item))
                     :else
                     (second item))))
          {}
          m))

(defn- fix-sub-map
  "Returns a map with its :sub entries' keys converted from keyword to
  integer.

  (RethinkDB converts them the opposite way when storing.)"
  [fragment]
  (if (contains? fragment :sub)
    (assoc fragment :sub
           (reduce (fn [acc item]
                     (assoc acc (-> (key item)
                                    name
                                    Integer/parseInt)
                            (cast-values (val item))))
                   {}
                   (:sub fragment)))
    fragment))

(defn get-classes
  [t]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        (r/get-all [(->table-name t)] {:index "type"})
        (r/run conn))))

;;
;; Public API
;;
(declare list-classes)
(defn class-exists?
  [c]
  (some (partial = (->table-name c)) (list-classes)))

(defn get-class
  [c]
  (class-action r/table c))

(defn get-metadata
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        #_(r/get-all [(->table-name c)] {:index "name"})
        (r/filter {:name (->table-name c)})
        (r/without [:id :name])
        (r/run conn)
        first)))

(defn get-fragment
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/sample 1)
        (r/without [:id])
        (r/run conn)
        first
        fix-sub-map
        cast-values)))

(defn list-classes
  []
  (vec (remove #{"classes"} (class-query r/table-list))))

(defn list-events
  []
  (with-open [conn (r/connect)]))

(def add-class (comp add-fragments add-metadata create-class))
(def reload-fragments (comp add-fragments delete-fragments))
(def remove-class (comp delete-metadata delete-class))
