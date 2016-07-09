(ns xyzzwhy.datastore
  (:require [clojure.string :as str]
            [environ.core :refer [env]]
            [pluralex.core :as pl]
            [rethinkdb.query :as r]))

(defonce db-name "xyzzwhy_corpora")

(defn table-name
  [c]
  (let [n (-> (if (map? c)
                (:classname c)
                c)
              name
              str
              (str/replace "-" "_"))]
    (pl/pluralize n)))

(declare class-action get-class-info)

(defn create-class
  [c]
  (class-action r/table-create c)
  (merge c (get-class-info c)))

(defn add-metadata
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        (r/insert {:id (:id c)
                   :name (:classname c)
                   :config (:config c)
                   :type (:type c)})
        (r/run conn)))
  c)

(defn add-fragments
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (table-name c))
        (r/insert (:fragment c))
        (r/run conn)))
  c)

;; Existing in a pre-macro state (probably)
(defn class-action
  [dsfn c]
  ((fn []
     (with-open [conn (r/connect)]
       (-> (r/db db-name)
           (dsfn (table-name c))
           (r/run conn))))))

(defn class-query
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

(defn delete-class
  [c]
  (let [c (if (or (string? c)
                  (keyword? c))
            (get-class-info (table-name c))
            c)]
    (class-action r/table-drop (:name c))
    c))

(defn delete-fragments
  [c]
  (let [c (if (or (string? c)
                  (keyword? c))
            (get-class-info (table-name c))
            c)]
    (with-open [conn (r/connect)]
      (-> (r/db db-name)
          (r/table (table-name (:classname c)))
          (r/delete)
          (r/run conn)))
    c))

(defn delete-metadata
  [c]
  (let [c (if (or (string? c)
                  (keyword? c))
            (get-class-info (table-name c))
            c)]
    (with-open [conn (r/connect)]
      (-> (r/db db-name)
          (r/table "classes")
          (r/filter (r/fn [table]
                      (r/eq (:id c) (r/get-field table :id))))
          (r/delete)
          (r/run conn)))
    c))

(defn initialize-database
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

(defn value
  "Casts the value of a MapEntry to the appropriate type since RethinkDB
  stores keywords as strings and sets as arrays."
  [entry]
  (cond
    (= :text (key entry)) (val entry)
    (= :prep (key entry)) (val entry)
    (= :article (key entry)) (val entry)
    (= :config (key entry)) (into #{} (map keyword (val entry)))
    (string? (val entry)) (keyword (val entry))
    :else
    (val entry)))

(defn submap
  "Returns a configuration map for a substitution."
  [smap]
  (into {} (mapcat #(assoc {} (key %) (value %)) smap)))


;;
;; Public API
;;
(declare list-classes)

(defn class-exists?
  [c]
  (some (partial = (table-name c)) (list-classes)))

(defn get-class
  [c]
  (class-action r/table c))

(defn get-class-info
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (table-name c))
        (r/info)
        (r/without [:type])
        (r/run conn))))

(defn get-fragment
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (table-name c))
        (r/sample 1)
        (r/without [:id])
        (r/run conn)
        first
        (update :sub #(mapv submap %)))))

(defn get-metadata
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        #_(r/get-all [(table-name c)] {:index "name"})
        (r/filter {:name c})
        (r/without [:id :name :type])
        (r/run conn)
        first
        submap)))

(defn classes
  []
  (vec (remove #{"classes"} (class-query r/table-list))))

(defn events
  []
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "classes")
        (r/filter {:type "event"})
        (r/pluck [:name])
        (r/run conn))))

(def add-class (comp add-metadata add-fragments create-class))
(def reload-fragments (comp add-fragments delete-fragments))
(def remove-class (comp delete-metadata delete-class))
