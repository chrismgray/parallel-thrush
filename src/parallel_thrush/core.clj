(ns parallel-thrush.core)

(defn pmapcat
  "mapcat meets pmap"
  [f coll]
  (apply concat (pmap f coll)))

(defn- alternating?
  "Returns true iff the sequence alternates between two things. Empty
   sequences, sequences of one thing, and sequences of two different
   things are also considered to be alternating."
  [coll]
  (cond (empty? coll)
        true
        (empty? (rest coll))
        true
        (empty? ((comp rest rest) coll))
        (apply not= coll)
        :else
        (let [[f & r] (partition 2 2 [] coll)]
          (and (apply not= f)
               (every? (partial = f) (butlast r))
               (or (= (last r) f)
                   (and
                    (= 1 (count (last r)))
                    (= (first (last r)) (first f))))))))

(def ^{:dynamic true} *parallel-thrush-partition-size* (+ 2 (.. Runtime getRuntime availableProcessors)))

(defmacro ||->>
  "Acts as ->>, but runs sections in parallel.  Uses two keywords,
   :merge and :split, to delimit the sections, beginning with an
   implicit :split and ending with an implicit :merge.  Sections
   beginning with :split are expected to consume and produce
   sequences, while sections beginning with :merge are only expected
   to consume sequences.  The sections beginning with :split are run
   in parallel, and the output sequence is fed into the sections
   beginning with :merge in the order in which it would appear if it
   had not been run in parallel.

   Since the keywords :merge and :split are special, they can only be
   used as functions if enclosed in parentheses.

   (||->> x :merge ...) is equivalent to (->> x ...).

   To parallelize the data, it is partitioned into groups of size
   *parallel-thrush-partition-size*.  This variable can be altered
   using the binding special form, but if it is not altered, it is
   best not to make assumptions about the size of the partitions.

   For example, the following will probably loop infinitely:

   (||->> (range)
          (filter odd?)
          (take 100)
          :merge
          (reduce +))

   while this will produce the correct result:

   (||->> (range)
          (filter odd?)
          :merge
          (take 100)
          (reduce +))"
  [x & body]
  (let [bodies (partition 2 2 (partition-by (complement #{:merge :split}) (cons :split body)))]
    (when (not (alternating? (filter #{:merge :split} (cons :split body))))
      (throw (Exception. ":merge and :split must alternate.")))
    (apply concat
           `(->> ~x)
           (for [[ops body] bodies]
             (if (= (last ops) :split)
               `((partition ~*parallel-thrush-partition-size* ~*parallel-thrush-partition-size* [])
                 (parallel-thrush.core/pmapcat
                  (fn [x#]
                    (->> x# ~@body))))
               `~body)))))

