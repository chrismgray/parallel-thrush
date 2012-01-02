(ns parallel-thrush.test.core
  (:use [parallel-thrush.core])
  (:use [midje.sweet]))

(facts "Equality"
  (||->> [1 2 3 4 5 6] (map inc)) => (->> [1 2 3 4 5 6] (map inc)))
