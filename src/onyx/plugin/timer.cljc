(ns onyx.plugin.timer
  (:require [clojure.core.async :refer [poll! timeout chan close!]]
            [clojure.set :refer [join]]
            [onyx.plugin.protocols :as p]
            [taoensso.timbre :refer [fatal info debug] :as timbre]))

(defn default-timer-fn
  [timestamp-ms interval-n]
  {:timestamp timestamp-ms
   :interval interval-n})

(defrecord TimerInput [interval interval-ms last-at timer-fn]
  p/Plugin

  (start [this event]
    this)

  (stop [this event] 
    this)

  p/Checkpointed
  (checkpoint [this]
    nil)

  (recover! [this _ checkpoint]
    this)

  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    false)

  p/Input
  (poll! [this _ poll-timeout-ms]
    (let [now (System/currentTimeMillis)
          expires-at (+ @last-at interval-ms)]
      (if (>= now expires-at)
        (do
          (vreset! last-at now)
          (let [n (vswap! interval inc)]
            (timer-fn now n)))
        (do
          (Thread/sleep (min (- expires-at now) poll-timeout-ms))
          nil)))))

(defn input [{:keys [onyx.core/task-map] :as event}]
  (map->TimerInput {:event event 
                    :interval-ms (get task-map :timer/interval-ms 1000)
                    :interval (volatile! 0)
                    :last-at (volatile! 0)
                    :timer-fn (or (:timer/fn event) default-timer-fn)}))

(defn inject-timer-fn
  [my-timer-fn]
  {:lifecycle/before-task-start 
   (fn [{:keys [onyx.core/task-map]} lifecycle]
     {:timer/fn my-timer-fn})})
