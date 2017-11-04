(ns onyx.windowing.reduce-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.test-helper :refer [load-config with-test-env]]
            [onyx.api]
            [onyx.static.uuid :refer [random-uuid]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]))

(def input
  [{:id 1  :age 21 :event-time #inst "2015-09-13T03:00:00.829-00:00"}
   {:id 2  :age 12 :event-time #inst "2015-09-13T03:04:00.829-00:00"}
   {:id 3  :age 3  :event-time #inst "2015-09-13T03:05:00.829-00:00"}
   {:id 4  :age 64 :event-time #inst "2015-09-13T03:06:00.829-00:00"}
   {:id 5  :age 53 :event-time #inst "2015-09-13T03:07:00.829-00:00"}
   {:id 6  :age 52 :event-time #inst "2015-09-13T03:08:00.829-00:00"}
   {:id 7  :age 24 :event-time #inst "2015-09-13T03:09:00.829-00:00"}
   {:id 8  :age 35 :event-time #inst "2015-09-13T03:15:00.829-00:00"}
   {:id 9  :age 49 :event-time #inst "2015-09-13T03:25:00.829-00:00"}
   {:id 10 :age 37 :event-time #inst "2015-09-13T03:45:00.829-00:00"}
   {:id 11 :age 15 :event-time #inst "2015-09-13T03:03:00.829-00:00"}
   {:id 12 :age 22 :event-time #inst "2015-09-13T03:56:00.829-00:00"}
   {:id 13 :age 83 :event-time #inst "2015-09-13T03:59:00.829-00:00"}
   {:id 14 :age 60 :event-time #inst "2015-09-13T03:32:00.829-00:00"}
   {:id 15 :age 35 :event-time #inst "2015-09-13T03:16:00.829-00:00"}])

(def expected-windows
  [[Double/NEGATIVE_INFINITY Double/POSITIVE_INFINITY 3]])

(def test-state (atom []))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as opts} extent-state]
  (when-not (= :job-completed event-type)
    (swap! test-state conj [lower-bound upper-bound extent-state])))

(def in-chan (atom nil))
(def in-buffer (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer
   :core.async/chan @in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(deftest min-test
  (let [id (random-uuid)
        config (load-config)
        env-config (assoc (:env-config config) :onyx/tenancy-id id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id id)
        batch-size 20
        workflow [[:in :reducer]]

        catalog
        [{:onyx/name :in
          :onyx/plugin :onyx.plugin.core-async/input
          :onyx/type :input
          :onyx/medium :core.async
          :onyx/batch-size batch-size
          :onyx/max-peers 1
          :onyx/doc "Reads segments from a core.async channel"}

         {:onyx/name :reducer
          ;:onyx/fn :clojure.core/identity
          :onyx/type :reduce
          :onyx/max-peers 1
          :onyx/batch-size batch-size}]

        windows
        [{:window/id :collect-segments
          :window/task :reducer
          :window/type :global
          :window/aggregation [:onyx.windowing.aggregation/min :age]
          :window/window-key :event-time
          :window/init 99}]

        triggers
        [{:trigger/window-id :collect-segments
          :trigger/id :sync
          :trigger/on :onyx.triggers/segment
          :trigger/threshold [15 :elements]
          :trigger/sync ::update-atom!}]

        lifecycles
        [{:lifecycle/task :in
          :lifecycle/calls ::in-calls}]]

    (reset! in-chan (chan (inc (count input))))
    (reset! in-buffer {})
    (reset! test-state [])

    (with-test-env [test-env [3 env-config peer-config]]
      
      (doseq [i input]
        (>!! @in-chan i))
      (close! @in-chan)

      (let [{:keys [job-id]} (onyx.api/submit-job
                              peer-config
                              {:catalog catalog
                               :workflow workflow
                               :lifecycles lifecycles
                               :windows windows
                               :triggers triggers
                               :task-scheduler :onyx.task-scheduler/balanced})
            _ (onyx.test-helper/feedback-exception! peer-config job-id)]
        (is (= expected-windows @test-state))))))