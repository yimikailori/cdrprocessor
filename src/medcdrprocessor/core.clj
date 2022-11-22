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
    (:import (java.io File)
             (java.util Timer TimerTask Date)))



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

(def ds-prod (delay (hikari/make-datasource
                        {:connection-timeout 10000
                         :idle-timeout 300000
                         :max-lifetime 900000
                         :minimum-idle 3
                         :maximum-pool-size 3
                         :adapter "postgresql"
                         :username (:username-prod @databaseInfo)
                         :password (:password-prod @databaseInfo)
                         :database-name (:dbname-prod @databaseInfo)
                         :server-name (:dburl-prod @databaseInfo)
                         :port-number (:dbport-prod @databaseInfo)})))


(defn filterlist [data]
  (when (or (= "DA130" (str (nth data 3) (nth data 4)))
            (= "DA135" (str (nth data 3) (nth data 4))))
    (list (nth data 0) (nth data 1) (nth data 2) (nth data 3) (nth data 4) (nth data 5) (nth data 6) (nth data 7)
          (nth data 8) (nth data 9) (nth data 10) (nth data 11)
          (nth data 12) (nth data 13) (nth data 14) (nth data 15) (nth data 16) (nth data 17) (nth data 18) (nth data 19)
          (nth data 20) (nth data 21) (nth data 22) (nth data 23) (nth data 24) (nth data 25))))

(defn- check-float [val]
  (if (empty? val) 0.0 (get (str/split val #",") 0)))
(defn- check-int [val]
  (if (empty? val) 0 (get (str/split val #",") 0)))
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
        _ (log/infof "Parsing data into [%s|%s|%s]"fname tempfile (second parsedata))]
    (condp = datacount
      ;refillDA
      53 (do
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
                                       (nth % 19 nil) (nth % 20 nil) (check-int (nth % 21 nil)) (check-float (nth % 24 nil)) (check-float (nth % 27 nil))
                                       (check-int (nth % 29 nil))
                                       (nth % 30 nil) fname]))
                    (csv/write-csv writer))))
           :da)
      ;;refillMA
      48 (do
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
                                (nth % 19 nil) (check-float (nth % 20 nil)) (nth % 21 nil) (check-float (nth % 22 nil)) (nth % 32 nil) (check-int (nth % 33 nil))
                                (nth % 37 nil) (check-int (nth % 38 nil)) (nth % 47 nil) fname))
                    (csv/write-csv writer))))
           :ma)
      (log/errorf "Unknown file data" from))))

;0,1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,24,43,44,46

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
      (cond (> (count b) col-count)
          (do
              (recur (first c) (rest c) (conj res (into [] (take col-count b)))))
          (or (not (= (count b) col-count)) (empty? (get b line)))
          (if (or (= "periodicAdjustment" (get b 0)) (= (count b) 1))
              (recur (first c) (rest c) res)
              (do
                  (log/errorf "inconsistent number of columns, value=%s|%s|%s" (count b) (get b line) b)
                  (recur (first c) (rest c) res)))
        :else (recur (first c) (rest c) (conj res b))))))

(defn- insert-multi-row
    "Given a table and a list of columns, followed by a list of column value sequences,
		return a vector of the SQL needed for the insert followed by the list of column
		value sequences. The entities function specifies how column names are transformed."
    [table columns values entities type]
    (let [nc (count columns)
          ;vcs (map count values)
          line (if (= type :da) 7 4)
          _ (log/debugf "%s count of columns=%s||first count of values=%s||last count of values=%s||line %s"type nc
                (count (first values)) (count (last values)) line)
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
                                 " VALUES (?,?,?,?,?::date,?,?,?,?::real,?,?::real,?::int,?,?,?,?,?::real,?::real,?,?,?,?,?,?)")]
                          newdata)
              (= type :sdp) (into [(str "INSERT INTO " (table->str table entities)
                                      (when (seq columns)
                                          (str " ( "
                                              (str/join ", " (map (fn [col] (col->str col entities)) columns))
                                              " )"))
                                      " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")]
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
      (log/infof "sql-params=%s" sql-params)
    (if-not (nil? sql-params)
        (if (jdbc/db-find-connection db)
            (jdbc/db-do-prepared db transaction? sql-params (assoc opts :multi? true))
            (doall
                (with-open [con (jdbc/get-connection db opts)]
                    (jdbc/db-do-prepared (jdbc/add-connection db con) transaction? sql-params
                        (assoc opts :multi? true)))
                (log/info "loadfile >>" tempfile)))
        (log/warnf "No :da or :ma  or :sdp in file [%s]" tempfile))))

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
      :sdp (insert-cols {:datasource @ds}                      ;
               :tbl_sdp_adj_refill_ma
               [:ADJUSTMENT_DR_TYPE :ADJUSTMENT_RECORD_TYPE :SDP_ID :CDR_ID :ACCOUNT_NUMBER :SUBSCRIBER_NUMBER
                :TRANSACTION_START_DATE :TRANSACTION_START_TIME :ADJUSTMENT_ACTION :ACCOUNT_BALANCE_BEF_TRANSACTION
                :ACCOUNT_BALANCE_AFT_TRANSACTION :ADJUSTMENT_OR_BONUS_AMOUNT :ORIGINAL_ADJUSTMENT_AMOUNT :SERVICE_CLASS_ID
                :ORIGIN_NODE_TYPE :ORIGIN_NODE_ID :ORIG_TRANSACTION_ID :TRANSACTION_TYPE :TRANSACTION_CODE :ORIGIN_OPERATOR_ID
                :DEDUCTED_AMOUNT :FNF_INDICATOR :INITIAL_AMOUNT_ADDED :PRIMARY_ADJUSTMENT_ACCOUNT_ID :SERVICE_OFFERING :BUFFER1
                :BUFFER2 :BUFFER3 :OFFER_ID :ADJ_OFFER_ACTION :ADJ_OFFER_REASON :OFFER_TYPE :PAM_SERVICE_ID :OFFER_PROVIDER_ID_POST
                :PRODUCT_ID :OFFER_START_TIME_POST :OFFER_EXPIRY_DATE_POST :OFFER_EXPIRY_DATE_PRE :SUPERVISION_EXPIRY_DATE_AFTER
                :CREDIT_CLEAR_PERIOD_AFTER :SERV_FEE_EXPIRY_DATE_AFTER :SERVICE_FEE_REMV_PERIOD_AFTER :ACCOUNT_FLAG_AFTER
                :NEW_SERVICE_CLASS_ID :VALUE_VOUCHER_EXP_DATE_AFTER :ACTIVATION_DATE
                ]
               ;:REFILLL_UN_BAR_DATE :REFILLL_UN_BAR_TIME
               ;:PROMO_PLAN_ID_AFTER :NEGATIVE_BAL_BARRING_DATE :NEW_ACCOUNT_GROUP_ID :NEW_SERVICE_OFFERINGS :SUBSCRIBER_CONVERGENT
               ;:NEW_COMMUNITY_ID1 :NEW_COMMUNITY_ID2 :NEW_COMMUNITY_ID3 :COUNTER_TYPE_ID :TOTAL_COUNTER_DELTA_VALUE
               ;:PERIOD_COUNTER_DELTA_VALUE :COST_SERVICE :PRESENTED_COST :CHARGING_FLAG :FNF_ACTION :FNF_NUMBER
               ;:ORIG_SERVICE_CLASS_AFTER :PAM_CLASS_ID_AFTER :PAM_SCHEDULE_ID_AFTER :PAM_PERIOD_ID_AFTER :DEFERRED_TO_DATE_AFTER
               ;:PAM_SERVICE_PRIORITY_AFTER :ADJUSTMENT_TRANS_TYPE :EOCNSELECTIONIFAFTER :ACCOUNT_PREPAID_EMPTY_LIMIT_AFT :CLEARED_ACCOUNT_VALUE
               ;:LAST_EVALUATION_DATE :SUCCESS_CODE :SUBSCRIBER_DELETED :CREDIT_CLEARED :ACCT_FLAGS_BEFORE :ACCT_FLAGS_AFTER
               array
               {:tempfile tempfile}
               :sdp)
    (throw (Exception. (format "cdr type (expected=[da ma sdp], found=%s)" file-cdr-type))))
  (when-not (= file-cdr-type :sdp)
      (io/delete-file (io/as-file tempfile))))

(defn text->map
  "Parses file and returns a maplist"
  [file tempdir archivedir data_type]
  (log/infof "file [%s] -> %s" file data_type)
  (let [fname (.getName file)
        archive (str archivedir fname)
        tempfile (if (= data_type :sdp) file
                     (str tempdir fname ".txt"))
        file-cdr-type (if (= data_type :sdp)
                          :sdp (parse-text file tempfile fname))
        fstream (slurp tempfile)
        streamarray (if (= data_type :sdp)
                        (map #(str/split % #"\|")
                            (str/split-lines fstream))
                        (map #(str/split % #",")
                            (str/split-lines fstream)))]

      ;;remove header
    (if (> (count streamarray) 0)
        (do
            (log/infof "Insert stream file [%s]"tempfile )
            (insert-stream (rest streamarray) tempfile file-cdr-type)
            (log/infof "File uploaded moving to archive [%s|%s]"tempfile archive))
        (log/errorf "File count is zero [%s|%s]" tempfile (first (rest streamarray))))
    #_((->> (rest streamarray)
         (partition 10000)
         (map (fn [batch] (insert-stream batch tempfile file-cdr-type)))
         (dorun))
    (io/delete-file (io/as-file tempfile)))

      ;e.g
      ;File sourceFile = new File("CoverPhoto.png");
      ;File destFile = new File("D:/Photo/ProfilePhoto.png");
      ;
      ;sourceFile.renameTo(destFile)
    (->> (File. archive)
         (.renameTo file))
    (count streamarray)))


(defn processfile [event filename tempdir archivedir datatype]
  (when (str/ends-with? (.getName filename) "csv")
    (log/info event filename)
      (utils/with-func-timed "processFile" filename
          (text->map filename tempdir archivedir datatype))))


(defn cpartitions
    [data type]
    (if (not= type :sdp)
        (let [{:keys [theyear startmonth endmonth newyear]} data
              start-month (str theyear "-"startmonth"-01 00:00:00")
              end-month (str (if (nil? newyear) theyear newyear) "-"endmonth"-01 00:00:00")
              partitions {:tbl_csdp_loans_cdrs_recon (str "tbl_csdp_loans_cdrs_recon_"theyear startmonth)
                          :tbl_csdp_recovery_cdrs_recon (str "tbl_csdp_recovery_cdrs_recon_"theyear startmonth)}]
            ;    CREATE TABLE public.tbl_csdp_recovery_cdrs_recon_202106 PARTITION OF
            ;    public.tbl_csdp_recovery_cdrs_recon
            ;    FOR VALUES FROM ('2021-06-01 00:00:00') TO ('2021-07-01 00:00:00');
            (doseq [tablename ["tbl_csdp_recovery_cdrs_recon"
                               "tbl_csdp_loans_cdrs_recon"]]
                (let [partitiontable   ((keyword tablename) partitions)
                      partition-exists (into {} (jdbc/query {:datasource @ds-prod}
                                                    [(str "select exists (SELECT FROM information_schema.tables  WHERE  table_name   = '" partitiontable "')")]))
                      ;_ (log/infof "partition-exists %s -> %s" partition-exists partitiontable)
                      ]
                    (when-not (boolean (:exists partition-exists))
                        (do
                            (log/infof (str "Initializing: CREATE TABLE "partitiontable " PARTITION OF "tablename" FOR VALUES FROM ('"start-month"') TO ('"end-month"')"))
                            (jdbc/execute! {:datasource @ds-prod}
                                [(str "CREATE TABLE "partitiontable " PARTITION OF "tablename" FOR VALUES FROM ('"start-month"') TO ('"end-month"'); GRANT select on " partitiontable " TO public;")]))
                        ))))
        (let [{:keys [theyear startmonth endmonth]} data
              start-month (str theyear "-"startmonth"-01")
              end-month (str theyear "-"endmonth"-01")]
            (let [tablename "tbl_sdp_adj_refill_ma"
                  partitiontable   (str "tbl_sdp_adj_refill_ma_"theyear startmonth)
                  partition-exists (into {} (jdbc/query {:datasource @ds}
                                                [(str "select exists (SELECT FROM information_schema.tables  WHERE  table_name   = '" partitiontable "')")]))
                  ;_ (log/infof "partition-exists %s -> %s" partition-exists partitiontable)
                  ]
                (when-not (boolean (:exists partition-exists))
                    (do
                        (log/infof (str "Initializing: CREATE TABLE "partitiontable " PARTITION OF "tablename" FOR VALUES FROM ('"start-month"') TO ('"end-month"')"))
                        (jdbc/execute! {:datasource @ds}
                            [(str "CREATE TABLE "partitiontable " PARTITION OF "tablename" FOR VALUES FROM ('"start-month"') TO ('"end-month"'); GRANT select on " partitiontable " TO public;")]))
                    )))))

(defn- create-dbpartitions [type]
    (try
        (let [date (str/split (f/unparse
                                  (f/formatters :date) (l/local-now)) #"-")
              y (Integer/parseInt (first date))
              m (Integer/parseInt (second date))
              mm (second date)
              d (Integer/parseInt (last date))
              newm (if (< m 10)
                       (str "0" (inc m))
                       (str (inc m)))]
            (do
                (cpartitions {:theyear y :startmonth mm :endmonth (if (= m 12) "01" newm) :newyear (if (= m 12) (+ y 1) y)} (when (= type :sdp) :sdp))
                (when (>= d 28)
                    (let [[y sm em] (if (= m 12)
                                        [(inc y) "01" "02"]
                                        [y mm newm])]
                        (log/infof "createPartitions(%s,%s,%s)" y sm em)
                        (cpartitions {:theyear y :startmonth sm :endmonth em} (when (= type :sdp) :sdp))))))
        (catch Exception e
            (log/error "cannotCreatePartitions() -> " (.getMessage e)))))

(defn starttask [type]
    (log/infof "Start create recon table partitions")
    (doto (Timer. "recon-timer" false) ; Do not run as a daemon thread.
        (.scheduleAtFixedRate (proxy [TimerTask] []
                                  (run []
                                      (create-dbpartitions type)))
            (Date.)
            (long 21600))))

(defn -main
  [& args]
  (let [config        (System/getProperty "medconfig")
        configdetails (read-string (slurp config))
        {:keys [incomingdir tempdir archivedir dbinfo datatype]} configdetails
        _             (log/infof "Details [%s]" configdetails)
        _             (reset! databaseInfo dbinfo)
        _ (starttask datatype)
        ]
    ;(watch-dir println (io/file path))
    (start-watch [{:path        incomingdir
                   :event-types [:create :modify]
                   :bootstrap   (fn [path] (let [_ (log/info "Starting to watch " path datatype)
                                                 files (file-seq (io/file path))]
                                             (loop [n (second files)
                                                    nx (drop 2 files)]
                                               (when-not (nil? n)
                                                 ;(log/info "=>>"n "=="nx)
                                                 (async/go (processfile :initial n (when-not (= datatype :sdp) tempdir)
                                                               archivedir datatype))
                                                 (recur (first nx) (rest nx))))))
                   :callback    (fn [event filename] (future (processfile event (io/as-file filename) (when-not (= datatype :sdp) tempdir)
                                                                 archivedir datatype)))
                   :options     {:recursive false}}])))




