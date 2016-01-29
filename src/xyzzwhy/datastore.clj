(ns xyzzwhy.datastore
  (:require [clojure.string :as str]
            [environ.core :refer [env]]
            [pluralex.core :refer [pluralize]]
            [rethinkdb.query :as r]))

(defonce db-name "xyzzwhy_corpora")

(defn- ->table-name
  [c]
  (-> (if (map? c)
        (:classname c)
        c)
      name
      str
      (str/replace "-" "_")))

(declare add-event add-config class-action)
(defn add-class
  [c]
  (class-action r/table-create c)
  (when (= (:type c) :event)
    (add-event c))
  (when (contains? c :config)
    (add-config c))
  c)

(defn- add-config
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "configs")
        (r/insert {:name (:classname c)
                   :config (:config c)})
        (r/run conn))))

(defn- add-event
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "events")
        (r/insert {:name (:classname c)})
        (r/run conn))))

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

(declare list-classes)
(defn class-exists?
  [c]
  (some (partial = (->table-name c)) (list-classes)))

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

(defn setup-database
  []
  (with-open [conn (r/connect)]
    (class-action r/table-create "events")
    (-> (r/db db-name)
        (r/table "events")
        (r/index-create "name" (r/fn [row]
                                 (r/get-field row :name)))
        (r/run conn))

    (class-action r/table-create "configs")
    (-> (r/db db-name)
        (r/table "configs")
        (r/index-create "name" (r/fn [row]
                                 (r/get-field row :name)))
        (r/run conn))))

(defn empty-class
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/delete)
        (r/run conn))))

(defn- fix-values [m]
  (reduce (fn [acc item]
            (assoc acc (first item)
                   (cond
                     (string? (second item)) (keyword (second item))
                     (vector? (second item)) (mapv keyword (second item))
                     (= :text (first item)) (second item)
                     :else
                     (second item))))
          {}
          m))

;; There has to be a better way to do this. An assoc inside of a reduce
;; inside of an assoc inside of a reduce inside of assoc? GTFO.
(defn- fix-sub-keys
  "Returns a map with its :sub entries' keys converted from keyword to
  integer.

  (RethinkDB converts them the opposite way when storing.)"
  [f]
  (if (contains? f :sub)
    (assoc f :sub
           (reduce (fn [acc item]
                     (assoc acc (-> (key item)
                                    name
                                    Integer/parseInt)
                            (fix-values (val item))))
                   {}
                   (:sub f)))
    f))


(defn get-class
  [c]
  (class-action r/table c))

(defn get-fragment
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table (->table-name c))
        (r/sample 1)
        (r/without [:id])
        (r/run conn)
        first
        fix-sub-keys)))

(defn list-classes
  []
  (vec (remove #{"events" "configs"} (class-query r/table-list))))

(defn list-events
  []
  (class-action r/table "events"))

(declare remove-event)
(defn remove-class
  [c]
  (let [c (if (string? c)
            (get-class c)
            c)]
    (class-action r/table-drop c)
    (when (= (:type c) :event)
      (remove-event c))
    c))

(defn remove-event
  [c]
  (with-open [conn (r/connect)]
    (-> (r/db db-name)
        (r/table "events")
        (r/filter (r/fn [event]
                    (r/eq (->table-name c) (r/get-field event :name))))
        (r/delete)
        (r/run conn))))

(def reset-class (comp add-fragments add-class remove-class))
