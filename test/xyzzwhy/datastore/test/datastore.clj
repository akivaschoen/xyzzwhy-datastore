(ns xyzzwhy.datastore.test.datastore
  (:require [xyzzwhy.datastore :as sut]
            [clojure.test :refer [are deftest is testing] :as t]))

(deftest test-table-name
  (are [table-name expected] (= (sut/table-name table-name) expected)
    :test "tests"
    :underline-work "underline_works" 
    {:classname :map-test-work} "map_test_works"))

(deftest test-submap
  (is (= (sut/submap {:class "location"
                      :config ["no-prep"]
                      :token 0})
         {:class :location
          :config #{:no-prep}
          :token 0})))

(deftest test-value
  (letfn [(mapentry [[k v]]
            (new clojure.lang.MapEntry k v))]
    (is (instance? String (sut/value (mapentry [:text "test"])))
        "Expected a string with key :text.")
    (is (instance? String (sut/value (mapentry [:prep "test"])))
        "Expected a string with key :prep.")
    (is (instance? String (sut/value (mapentry [:article "test"])))
        "Expected a string with key :article.")
    (is (instance? clojure.lang.PersistentHashSet
                   (sut/value (mapentry [:config ["this" "that"]])))
        "Expected a two-item set with key :config.")
    (is (instance? clojure.lang.Keyword (sut/value (mapentry [:key "test"])))
        "Expected a keyword with an unknown key that has a string value.")
    (is (instance? clojure.lang.Keyword (sut/value (mapentry [:default :test])))
        "Expected a keyword with an unknown key that has a keyword value.")))
