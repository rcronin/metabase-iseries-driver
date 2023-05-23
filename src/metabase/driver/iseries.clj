(ns metabase.driver.iseries
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [honeysql.core :as hsql]
            [java-time :as t]
            [metabase.driver :as driver]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]] 
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.util :as u]
            [metabase.util.date-2 :as du]
            [metabase.util.honeysql-extensions :as hx]
            [metabase.util.ssh :as ssh]
            [metabase.util.i18n :refer [trs]])
  (:import [java.sql ResultSet Types]
           java.util.Date)
  (:import [java.sql ResultSet Time Timestamp Types]
           [java.util Calendar Date TimeZone]
           [java.time Instant LocalDateTime OffsetDateTime OffsetTime ZonedDateTime LocalDate LocalTime] 
           org.joda.time.format.DateTimeFormatter))

(driver/register! :iseries, :parent #{:sql-jdbc})


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :iseries [_] "DB2 for i")

(defmethod driver/humanize-connection-error-message :iseries [_ message]
    (condp re-matches message
    #"^FATAL: database \".*\" does not exist$"
    :database-name-incorrect

    #"^No suitable driver found for.*$"
    :invalid-hostname

    #"^Connection refused. Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.$"
    :cannot-connect-check-host-and-port

    #"^FATAL: role \".*\" does not exist$"
    :username-incorrect

    #"^FATAL: password authentication failed for user.*$"
    :password-incorrect

    #"^FATAL: .*$" ; all other FATAL messages: strip off the 'FATAL' part, capitalize, and add a period
    (let [[_ message] (re-matches #"^FATAL: (.*$)" message)]
      (str (str/capitalize message) \.))
      
    message))

(defmethod driver.common/current-db-time-date-formatters :iseries [_]     ;; "2019-09-14T18:03:20.679658000-00:00"
;;  (println "current-db-time-date-formatters")
  (mapcat
   driver.common/create-db-time-formatters
   ["yyyy-MM-dd HH:mm:ss"
    "yyyy-MM-dd HH:mm:ss.SSS"
    "yyyy-MM-dd'T'HH:mm:ss.SSS"
    "yyyy-MM-dd HH:mm:ss.SSSZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
    "yyyy-MM-dd HH:mm:ss.SSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"
    "yyyy-MM-dd HH:mm:ss.SSSSSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZZ"
    "yyyy-MM-dd HH:mm:ss.SSSSSSSSSZZ"
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSZZ"]))

(defmethod driver.common/current-db-time-native-query :iseries [_]
;;  (println "current-db-time-native-query")
  "SELECT TO_CHAR(CURRENT TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') FROM SYSIBM.SYSDUMMY1")       ;; "SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")

(defmethod driver/current-db-time :iseries [& args]
;;  (println "current-db-time")
  (apply driver.common/current-db-time args))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- date-format [format-str expr] (hsql/call :varchar_format expr (hx/literal format-str)))
(defn- str-to-date [format-str expr] (hsql/call :to_date expr (hx/literal format-str)))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:iseries :default]        [_ _ expr] expr)
(defmethod sql.qp/date [:iseries :minute]         [_ _ expr] (trunc-with-format "YYYY-MM-DD HH24:MI" expr))
(defmethod sql.qp/date [:iseries :minute-of-hour] [_ _ expr] (hsql/call :minute expr))
(defmethod sql.qp/date [:iseries :hour]           [_ _ expr] (trunc-with-format "YYYY-MM-DD HH24" expr))
(defmethod sql.qp/date [:iseries :hour-of-day]    [_ _ expr] (hsql/call :hour expr))
(defmethod sql.qp/date [:iseries :day]            [_ _ expr] (hsql/call :date expr))
(defmethod sql.qp/date [:iseries :day-of-month]   [_ _ expr] (hsql/call :day expr))
(defmethod sql.qp/date [:iseries :week]           [_ _ expr] (hx/- expr (hsql/raw (format "%d days" (int (hx/- (hsql/call :dayofweek expr) 1))))))
(defmethod sql.qp/date [:iseries :month]          [_ _ expr] (str-to-date "YYYY-MM-DD" (hx/concat (date-format "YYYY-MM" expr) (hx/literal "-01"))))
(defmethod sql.qp/date [:iseries :month-of-year]  [_ _ expr] (hsql/call :month expr))
(defmethod sql.qp/date [:iseries :quarter]        [_ _ expr] (str-to-date "YYYY-MM-DD" (hsql/raw (format "%d-%d-01" (int (hx/year expr)) (int ((hx/- (hx/* (hx/quarter expr) 3) 2)))))))
(defmethod sql.qp/date [:iseries :year]           [_ _ expr] (hsql/call :year expr))

(defmethod sql.qp/date [:iseries :day-of-year] [driver _ expr] (hsql/call :dayofyear expr))

(defmethod sql.qp/date [:iseries :week-of-year] [_ _ expr] (hsql/call :week expr))

(defmethod sql.qp/date [:iseries :quarter-of-year] [driver _ expr] (hsql/call :quarter expr))

(defmethod sql.qp/date [:iseries :day-of-week] [driver _ expr] (hsql/call :dayofweek expr))

(defmethod sql.qp/add-interval-honeysql-form :iseries [_ hsql-form amount unit]
  (hx/+ (hx/->timestamp hsql-form) (case unit
                              :second  (hsql/raw (format "  %d seconds" (int amount)))
                              :minute  (hsql/raw (format "  %d minutes" (int amount)))
                              :hour    (hsql/raw (format "  %d hours" (int amount)))
                              :day     (hsql/raw (format "  %d days" (int amount)))
                              :week    (hsql/raw (format "  %d days" (int (hx/* amount (hsql/raw 7)))))
                              :month   (hsql/raw (format "  %d months" (int amount)))
                              :quarter (hsql/raw (format "  %d months" (int (hx/* amount (hsql/raw 3)))))
                              :year    (hsql/raw (format "  %d years" (int amount))))))

(defmethod sql.qp/unix-timestamp->honeysql [:iseries :seconds] [_ _ expr]
  (hx/+ (hsql/raw "timestamp('1970-01-01 00:00:00')") (hsql/raw (format "%d seconds" (int expr))))

(defmethod sql.qp/unix-timestamp->honeysql [:iseries :milliseconds] [driver _ expr]
  (hx/+ (hsql/raw "timestamp('1970-01-01 00:00:00')") (hsql/raw (format "%d seconds" (int (hx// expr 1000)))))))

(def ^:private now (hsql/raw "current timestamp"))

(defmethod sql.qp/current-datetime-honeysql-form :iseries [_] now)

;; DB2 ibm i doesn't support `TRUE`/`FALSE`; use `1`/`0`, respectively; convert these booleans to numbers.
(defmethod sql.qp/->honeysql [:iseries Boolean]
  [_ bool]
  (if bool 1 0))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql date workarounds                                 |
;;; +----------------------------------------------------------------------------------------------------------------+

;; (.getObject rs i LocalDate) doesn't seem to work, nor does `(.getDate)`; ;;v0.34.x
;; Merged from vertica.clj e sqlite.clj.
;; Fix to Invalid data conversion: Wrong result column type for requested conversion. ERRORCODE=-4461

(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/DATE]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [s (.getString rs i)]
      (let [t (du/parse s)]
        (log/tracef "(.getString rs %d) [DATE] -> %s -> %s" i s t)
        t))))

(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/TIME]
  [_driver ^ResultSet rs _rsmeta ^Long i]
  (fn read-time []
    (when-let [s (.getString rs i)]
      (let [t (du/parse s)]
        (log/tracef "(.getString rs %d) [TIME] -> %s -> %s" i s t)
        t))))

(defmethod sql-jdbc.execute/read-column-thunk [:iseries Types/TIMESTAMP]
  [_driver ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [s (.getString rs i)]
      (let [t (du/parse s)]
        (log/tracef "(.getString rs %d) [TIMESTAMP] -> %s -> %s" i s t)
        t))))

(defmethod sql-jdbc.execute/set-parameter [:iseries OffsetDateTime]
  [driver ps i t]
  (sql-jdbc.execute/set-parameter driver ps i (t/sql-timestamp (t/with-offset-same-instant t (t/zone-offset 0)))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.conn/connection-details->spec :iseries
  [_ {:keys [host port dbname]
      :or   {host "localhost", port 3386, dbname ""}
      :as   details}]
  (-> (merge {:classname "com.ibm.as400.access.AS400JDBCDriver"   ;; must be in classpath
          :subprotocol "as400"
          :subname (str "//" host ":" port "/" dbname)}                    ;; :subname (str "//" host "/" dbname)}   (str "//" host ":" port "/" (or dbname db))}
         (dissoc details :host :port :dbname))
  (sql-jdbc.common/handle-additional-options details, :separator-style :semicolon)))

(defmethod driver/can-connect? :iseries [driver details]
  (let [connection (sql-jdbc.conn/connection-details->spec driver (ssh/include-ssh-tunnel! details))]
    (= 1 (first (vals (first (jdbc/query connection ["SELECT 1 FROM SYSIBM.SYSDUMMY1"])))))))

(defmethod sql-jdbc.sync/database-type->base-type :iseries [_ database-type]
  ({:BIGINT       :type/BigInteger    ;; Mappings for DB2 types to Metabase types.
    :BINARY       :type/*             ;; See the list here: https://docs.tibco.com/pub/spc/4.0.0/doc/html/ibmdb2/ibmdb2_data_types.htm
    :BLOB         :type/*
    :BOOLEAN      :type/Boolean
    :CHAR         :type/Text
    :NCHAR        :type/Text
    :CLOB         :type/Text
    :DATALINK     :type/*
    :DATE         :type/Date
    :DBCLOB       :type/Text
    :DECIMAL      :type/Decimal
    :DECFLOAT     :type/Decimal
    :DOUBLE       :type/Float
    :FLOAT        :type/Float
    :GRAPHIC      :type/Text
    :INTEGER      :type/Integer
    :NUMERIC      :type/Decimal
    :REAL         :type/Float
    :ROWID        :type/*
    :SMALLINT     :type/Integer
    :TIME         :type/Time
    :TIMESTAMP    :type/DateTime
    :VARCHAR      :type/Text
    :NVARCHAR     :type/Text
    :VARGRAPHIC   :type/Text
    :XML          :type/Text
    (keyword "CHAR () FOR BIT DATA")      :type/*
    (keyword "LONG VARCHAR")              :type/*
    (keyword "LONG VARCHAR FOR BIT DATA") :type/*
    (keyword "LONG VARGRAPHIC")           :type/*
    (keyword "VARCHAR () FOR BIT DATA")   :type/*} database-type))

(defmethod sql-jdbc.sync/excluded-schemas :iseries [_]
  #{"SQLJ"
    "QCCA"
    "QCLUSTER"
    "QDNS"
    "QDSNX"
    "QFNTCPL"
    "QFNTWT"
    "QFPNTWE"
    "QGDDM"
    "QICSS"
    "QICU"
    "QIWS"
    "QJRNL"
    "QMSE"
    "QNAVSRV"
    "QNEWNAVSRV"
    "QPASE"
    "QPFRDATA"
    "QQALIB"
    "QRCL"
    "QRECOVERY"
    "QRPLOBJ"
    "QSHELL"
    "QSMP"
    "QSOC"
    "QSPL"
    "QSR"
    "QSRV"
    "QSRVAGT"
    "QSYSCGI"
    "QSYSDIR"
    "QSYSINC"
    "QSYSLOCALE"
    "QSYSNLS"    
    "QSYS2"
    "QTEMP"
    "QUSRBRM"
    "QUSRDIRDB"
    "QUSRTEMP"
    "QUSRTOOL"
    "QTCP"
    "QSYS"    
    "SYSCAT"
    "SYSFUN"
    "SYSIBM"
    "SYSIBMADM"
    "SYSIBMINTERNAL"
    "SYSIBMTS"
    "SPOOLMAIL"
    "SYSPROC"
    "SYSPUBLIC"
    "SYSTOOLS"
    "SYSSTAT"
    "QHTTPSVR"
    "QUSRSYS"})

(defmethod sql-jdbc.execute/set-timezone-sql :iseries [_]
  "SET SESSION TIME ZONE = %s")

(defn- materialized-views
  "Fetch the Materialized Views DB2 for i"
  [database]  
  (log/info (trs "Fetch the Materialized Views db2 for i"))
  (try (set (jdbc/query (sql-jdbc.conn/db->pooled-connection-spec database)
      ["select schema, table, case when table_text = '' then null else table_text end as description from etllib.metabase_table_metadata allowed join qsys2.systables on schema = table_schema and table_name = table"]))
       (catch Throwable e
         (log/error e (trs "Failed to fetch materialized views for DB2 for i")))))

(defmethod driver/describe-database :iseries
  [driver database]
  (-> ((get-method driver/describe-database :sql-jdbc) driver database)
      (update :tables set/union (materialized-views database))      
      ;;(log/info (trs "tables apres materialized: {0}" (materialized-views database)))
      ))
        
;; instead of returning a CLOB object, return the String. (#9026)
;; (defmethod sql-jdbc.execute/read-column [:iseries Types/CLOB] [_ _, ^ResultSet resultset, _, ^Integer i]
;;   (println "XXXXX read-column: " i)
;;   (.getString resultset i))

;; (defmethod unprepare/unprepare-value [:iseries Date] [_ value]
;;   (println "XXXXX unprepare/unprepare-value: " value)
;;   (format "timestamp '%s'" (du/format-date "yyyy-MM-dd hh:mm:ss.SSS ZZ" value)))