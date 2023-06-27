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
  (-> (update :tables set/union (materialized-views database))      
      ;;(log/info (trs "tables apres materialized: {0}" (materialized-views database)))
      ))


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