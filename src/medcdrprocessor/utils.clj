(ns medcdrprocessor.utils
	(:require [clojure.tools.logging :as log]))

(defmacro with-func-timed
	"macro to execute blocks that need to be timed and may throw exceptions"
	[tag filename & body]
	`(let
		 [fn-name# (str "do" (.toUpperCase (subs ~tag 0 1)) (subs ~tag 1))
			start-time# (System/currentTimeMillis)
			e-handler# (fn [err# fname#] (log/errorf err# "!%s -> %s|%s[%s]" fname# (.getNextException err#) (.getMessage err#) ~filename) :error)
			return-val# (try
							~@body
							(catch Exception se#
								(e-handler# se# fn-name#)))]
		 ;(log/infof "%s -> %s" fn-name# return-val#)
		 (log/infof "callProf|%s|%s -> %s|%s" (- (System/currentTimeMillis) start-time#) fn-name# return-val# ~filename)
		 return-val#))
