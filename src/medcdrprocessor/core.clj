(ns medcdrprocessor.core
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clj-time.local :as l]
            [clj-time.format :as f]
            [clojure.tools.logging :as log]
            [clojure-watch.core :refer [start-watch]]
            [medcdrprocessor.utils :as utils]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.csv :as csv]
            [hikari-cp.core :as hikari]

            [clojure.core.async :as async])
  (:import (java.io File)))



(def databaseInfo (atom {}))

(def ds (delay (hikari/make-datasource
                     {:connection-timeout 10000
                      :idle-timeout 300000
                      :max-lifetime 900000
                      :minimum-idle 10
                      :maximum-pool-size  (:max-pool-size @databaseInfo)
                      :adapter "postgresql"
                      :username (:username @databaseInfo)
                      :password (:password @databaseInfo)
                      :database-name (:dbname @databaseInfo)
                      :server-name (:dburl @databaseInfo)
                      :port-number (:dbport @databaseInfo)})))


(defn filterlist [data]
  (when (or (= "DA130" (str (nth data 3) (nth data 4)))
            (= "DA135" (str (nth data 3) (nth data 4))))
    (list (nth data 0) (nth data 1) (nth data 2) (nth data 3) (nth data 4) (nth data 5) (nth data 6) (nth data 7)
          (nth data 8) (nth data 9) (nth data 10) (nth data 11)
          (nth data 12) (nth data 13) (nth data 14) (nth data 15) (nth data 16) (nth data 17) (nth data 18) (nth data 19)
          (nth data 20) (nth data 21) (nth data 22) (nth data 23) (nth data 24) (nth data 25))))

(defn- check-float [val]
  (if (empty? val) 0.0 val))
(defn- check-int [val]
  (if (empty? val) 0 val))
;CREATE TABLE public.tbl_tmp_reco_loan_cdrs_20191105 PARTITION OF public.tbl_tmp_reco_loan_cdrs
;    FOR VALUES FROM ('2019-11-05 00:00:00+01') TO ('2019-11-06 00:00:00+01');
(defn parse-text [from tempfile fname]
  "Convert a text into rows of columns"
  (let [reader (io/reader from)
        dataread (csv/read-csv reader :separator \>)
        datacount (count (first dataread))
        parsedata (if (< (count (last dataread)) 24)
                    (do
                        (log/infof "Clean last streamarray with unknown number of column %s|%s" (last dataread) tempfile )
                        (into [] (remove #{(last dataread)} dataread)))
                    dataread)
        _ (log/infof "Parsing data into [%s|%s]"tempfile (first parsedata))]
    (condp = datacount
      ;refillDA
      51 (do
           (with-open [writer (io/writer tempfile)]
             (doall
               (->> parsedata
                    (map #(filterlist [(nth % 0 nil) (nth % 1 nil) (nth % 2 nil) (nth % 4 nil) (check-int (nth % 5 nil))
                                       (check-float (nth % 6 nil))
                                       (check-float (nth % 7 nil)) (nth % 8 nil) (nth % 9 nil)
                                       #_(-> (f/formatter "yyyyMMddHHmmss")
                                             (f/parse (str (nth % 8 nil) (nth % 9 nil)))
                                             (l/format-local-time :mysql)
                                             (try (catch Exception _ nil)))
                                       (nth % 10 nil) (nth % 11 nil) (nth % 12 nil)
                                       (nth % 13 nil) (nth % 14 nil) (nth % 15 nil) (nth % 16 nil) (nth % 17 nil) (nth % 18 nil)
                                       (nth % 19 nil) (nth % 20 nil) (nth % 21 nil) (check-float (nth % 22 nil)) (check-float (nth % 25 nil)) (nth % 27 nil)
                                       (nth % 28 nil) fname]))
                    (csv/write-csv writer))))
           :da)
      ;;refillMA
      46 (do
           (with-open [writer (io/writer tempfile)]
             (doall
               (->> parsedata
                    (map #(list (nth % 0 nil) (nth % 1 nil) (nth % 3 nil) (nth % 5 nil)
                                (nth % 6 nil) (nth % 7 nil)
                                #_(-> (f/formatter "yyyyMMddHHmmss")
                                    (f/parse (str (nth % 6 nil) (nth % 7 nil)))
                                    (l/format-local-time :mysql)
                                    (try (catch Exception _ nil)))
                                (nth % 8 nil)
                                (nth % 9 nil) (check-float (nth % 12 nil)) (nth % 13 nil) (check-float (nth % 15 nil)) (check-int (nth % 16 nil)) (nth % 17 nil) (nth % 18 nil)
                                (nth % 19 nil) (check-float (nth % 20 nil)) (nth % 21 nil) (check-float (nth % 22 nil)) (nth % 32 nil) (nth % 33 nil)
                                (nth % 35 nil) (check-int (nth % 36 nil)) (nth % 45 nil) fname))
                    (csv/write-csv writer))))
           :ma)
      (log/errorf "Unknown file data" from))))



(defn- table->str
  "Transform a table spec to an entity name for SQL. The table spec may be a
  string, a keyword or a map with a single pair - table name and alias."
  [table entities]
  (let [entities (or entities identity)]
    (if (map? table)
      (let [[k v] (first table)]
        (str (jdbc/as-sql-name entities k) " " (jdbc/as-sql-name entities v)))
      (jdbc/as-sql-name entities table))))

(defn- col->str
  "Transform a column spec to an entity name for SQL. The column spec may be a
  string, a keyword or a map with a single pair - column name and alias."
  [col entities]
  (if (map? col)
    (let [[k v] (first col)]
      (str (jdbc/as-sql-name entities k) " AS " (jdbc/as-sql-name entities v)))
    (jdbc/as-sql-name entities col)))

(defn- clean-data [values col-count line]
  (loop [b (first values)
         c (rest values)
         res []]
    (if (nil? b)
      res
      (if (or (not (= (count b) col-count)) (empty? (get b line)))
        (do
          ;(log/errorf "inconsistent number of columns, value=%s" b)
          (recur (first c) (rest c) res))
        (recur (first c) (rest c) (conj res b))))))

(defn- insert-multi-row
    "Given a table and a list of columns, followed by a list of column value sequences,
		return a vector of the SQL needed for the insert followed by the list of column
		value sequences. The entities function specifies how column names are transformed."
    [table columns values entities type]
    (let [nc (count columns)
          ;vcs (map count values)
          line (if (= type :da) 7 4)
          _ (log/debugf "%s count of columns=%s||first count of values=%s||last count of values=%s||line %s"type nc (count (first values)) (count (last values)) line)
          newdata (clean-data values nc line)]
      (when (not (empty? newdata))
        (log/infof "inserting data %s" [table entities type])
       (cond (= type :da)(into [(str "INSERT INTO " (table->str table entities)
                                      (when (seq columns)
                                        (str " ( "
                                             (str/join ", " (map (fn [col] (col->str col entities)) columns))
                                             " )"))
                                      " VALUES (?,?,?,?,?::int,?::real,?::real,?::date,?,?,?,?,?,?,?,?,?,?,?,?,?,?::real,?::real,?,?,?)")]
                                newdata)
         (= type :ma) (into [(str "INSERT INTO " (table->str table entities)
                                   (when (seq columns)
                                     (str " ( "
                                          (str/join ", " (map (fn [col] (col->str col entities)) columns))
                                          " )"))
                                   " VALUES (?,?,?,?,?::date,?,?,?,?::real,?,?::real,?::int,?,?,?,?,?::real,?::real,?,?,?,?::int,?,?)")]
                             newdata)))))

(defn- insert-cols
  "Given a database connection, a table name, a sequence of columns names, a
  sequence of vectors of column values, one per row, and an options map,
  insert the rows into the database."
  [db table cols values opts type]
  (let [_ (log/infof "filtering data %s" [db table opts type])
        {:keys [entities transaction? tempfile] :as opts}
        (merge {:entities identity :transaction? true} (when (map? db) db) opts)
        sql-params (insert-multi-row table cols values entities type)]
    ;(log/infof "sql-params=%s" sql-params)
    (if (jdbc/db-find-connection db)
      (jdbc/db-do-prepared db transaction? sql-params (assoc opts :multi? true))
      (doall
        (with-open [con (jdbc/get-connection db opts)]
          (jdbc/db-do-prepared (jdbc/add-connection db con) transaction? sql-params
                        (assoc opts :multi? true)))
          (log/info "loadfile >>" tempfile)))))

;(insert-cols db table cols-or-row [values-or-opts] {})

(defn insert-stream
  [array tempfile file-cdr-type]
  (log/infof "insert-stream [%s|%s]" tempfile file-cdr-type)
  (condp = file-cdr-type
    :da (insert-cols {:datasource @ds}                      ;
                     :tbl_air_refill_da
                     [:origin_node_type :origin_host_name :origin_transaction_id
                      :da_ua_type :da_ua_id :da_ua_account_balance_before :da_ua_account_balance_after
                      :transaction_start_date :transaction_start_time :current_service_class
                      :voucher_based_refill :transaction_type :transaction_code :transaction_currency
                      :refill_type :voucher_serial_number :voucher_group_id :subscriber_number
                      :account_number :external_data1 :external_data2
                      :refill_division_amount :dedicated_account_unit :account_expiry_date :primary_recharge_account_id
                      :filename]
                     array
                     {:tempfile tempfile}
                     :da)
    :ma (insert-cols {:datasource @ds}                      ;
                     :tbl_air_refill_ma
                     [:origin_node_type :origin_host_name :origin_transaction_id :host_name
                      :transaction_start_date :transaction_start_time :current_service_class :voucher_based_refill
                      :transaction_amount :transaction_currency :refill_division_amount
                      :refill_type :voucher_serial_number :voucher_group_key :account_number :subscriber_number
                      :account_balance_before :account_balance_after :external_data1 :external_data2
                      :cell_identifier :primary_recharge_account_id :location_number
                      :filename]
                     array
                     {:tempfile tempfile}
                     :ma)
    (throw (Exception. (format "cdr type (expected=[da ma], found=%s)" file-cdr-type))))
  (io/delete-file (io/as-file tempfile)))

(defn text->map
  "Parses file and returns a maplist"
  [file tempdir archivedir]
  (log/info "file ->" file)
  (let [fname (.getName file)
        archive (str archivedir fname)
        tempfile (str tempdir fname ".txt")
        file-cdr-type (parse-text file tempfile fname)
        fstream (slurp tempfile)
        streamarray (map #(str/split % #",")
                         (str/split-lines fstream))]

      ;;remove header
    (if (> (count streamarray) 0)
        (do
            (log/infof "Insert stream file [%s]"tempfile)
            (insert-stream (rest streamarray) tempfile file-cdr-type)
            (log/infof "File uploaded now cleaning [%s]"tempfile))
        (log/errorf "File count is zero [%s|%s]" tempfile (first (rest streamarray))))
    #_((->> (rest streamarray)
         (partition 10000)
         (map (fn [batch] (insert-stream batch tempfile file-cdr-type)))
         (dorun))
    (io/delete-file (io/as-file tempfile)))

    (->> (File. archive)
         (.renameTo file))
    (count tempfile)))

(defn processfile [event filename tempdir archivedir]
  (when (str/ends-with? (.getName filename) "csv")
    (log/info event filename)
      (utils/with-func-timed "processFile" filename
          (text->map filename tempdir archivedir))))

(defn -main
  [& args]
  (let [config (System/getProperty "medconfig")
        configdetails (read-string (slurp config))
        {:keys [incomingdir tempdir archivedir dbinfo]} configdetails
        _ (log/infof "Details [%s]" configdetails)
        _ (reset! databaseInfo dbinfo)]
    ;(watch-dir println (io/file path))
    (start-watch [{:path        incomingdir
                   :event-types [:create :modify]
                   :bootstrap   (fn [path] (let [_ (log/info "Starting to watch " path)
                                                 files (file-seq (io/file path))]
                                             (loop [n (second files)
                                                    nx (drop 2 files)]
                                               (when-not (nil? n)
                                                 ;(log/info "=>>"n "=="nx)
                                                 (async/go (processfile :initial n tempdir archivedir))
                                                 (recur (first nx) (rest nx))))))
                   :callback    (fn [event filename] (future (processfile event (io/as-file filename) tempdir archivedir)))
                   :options     {:recursive false}}])))




