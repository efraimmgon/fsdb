(ns fsdb.core
  (:require
    clojure.edn
    clojure.pprint
    [clojure.java.io :as io]
    [me.raynes.fs :as fs])
  (:import
    java.time.Instant))

;;; ----------------------------------------------------------------------------
;;; Filesystem-based Database
;;; ----------------------------------------------------------------------------

;;; ----------------------------------------------------------------------------
;;; Utils
;;; ----------------------------------------------------------------------------


;;; All the data is placed inside the resources/db folder, by default.
(def db-dir (io/file fs/*cwd* "resources" "db"))
(def settings-path (io/file db-dir "settings.edn"))


(defn load-edn
  "Load edn from an io/reader source (filename or io/resource)."
  [source]
  (try
    (with-open [r (io/reader source)]
      (clojure.edn/read
        {:readers {'inst #(Instant/parse %)}}
        (java.io.PushbackReader.
          r)))

    (catch java.io.IOException e
      (printf "Couldn't open '%s': %s\n" source (.getMessage e)))
    (catch RuntimeException e
      (printf "Error parsing edn file '%s': %s\n" source (.getMessage e)))))


(defn save-edn!
  "Save edn data to file.
  Opts is a map of #{:pretty-print}."
  ([file data] (save-edn! file data nil))
  ([file data opts]
   (with-open [wrt (io/writer file)]
     (binding [*out* wrt]
       (if (:pretty-print? opts)
         (clojure.pprint/pprint data)
         (prn data))))
   data))


(declare settings)

(defn table-path
  "Returns the table's path to the dir where records are stored."
  [tname]
  (get-in @settings [:tables tname :path]))


(defn table-file
  "Return the io/file for the table dir/record, if it exists."
  ([tname]
   (table-file tname nil))
  ([tname id]
   (when-let [path (table-path tname)]
     (let [file (or (and id (io/file path (str id)))
                  (io/file path))]
       (when (fs/exists? file)
         file)))))


(defn use-qualified-keywords?
  "Returns true if the settings file has the :use-qualified-keywords? 
   set to `true` It's `false` by default."
  []
  (:use-qualified-keywords? @settings))


(defn get-record-key
  "Returns the keyword to be used in the table record."
  [tname field]
  (if (use-qualified-keywords?)
    (keyword (name tname) (name field))
    field))


;;; ----------------------------------------------------------------------------
;;; Settings
;;; ----------------------------------------------------------------------------


; Management of id increment

(def settings
  "DB settings."
  (atom {}))


(defn load-settings! []
  (reset! settings (load-edn settings-path)))


(defn save-settings! []
  (save-edn! settings-path @settings {:pretty-print? true}))


(defn next-id!
  "Increment the table's counter and return the incremented number."
  [tname]
  (swap! settings update-in
    [:tables tname :counter] inc)
  (future (save-settings!))
  (get-in @settings
    [:tables tname :counter]))


(defn setup!
  "Checks if the db path and the settings file are set, otherwise will do it."
  [& [opts]]
  (when-not (fs/exists? db-dir)
    (fs/mkdirs db-dir))
  (when-not (fs/exists? settings-path)
    (let [opts (merge {:use-qualified-keywords? false}
                 opts)]
      (save-edn! settings-path opts)))
  (load-settings!))


(defn reset-db!
  "Deletes all data and settings."
  []
  (fs/delete-dir db-dir)
  (setup!))


(setup!)


;;; ----------------------------------------------------------------------------
;;; CREATE, DELETE TABLE
;;; ----------------------------------------------------------------------------


(defn create-table!
  "Creates the settings for the table. These settings will be used when 
  querying the table."
  [tname]
  (when-not (table-path tname)
    (let [table-path (io/file db-dir (name tname))
          config {:path (str table-path)
                  :counter 0}]
      ;; Create a the dir where the records will be saved.
      (fs/mkdir table-path)
      ;; Update the settings with the new table config.
      (save-edn!
        settings-path
        (swap! settings assoc-in [:tables tname] config)))))


(defn delete-table!
  "Deletes all data and settings related to the given table."
  [tname]
  (when-let [dir (table-path tname)]
    (fs/delete-dir dir)
    (save-edn!
      settings-path
      (swap! settings update :tables dissoc tname))))


;;; ----------------------------------------------------------------------------
;;; GET, SAVE, DELETE
;;; ----------------------------------------------------------------------------
;;; All files are expected to contain edn objects, so we just use
;;; clojure.edn/read when loading them from the file.


(defn get-by-id
  "Reads and returns the contents of the given file."
  [tname id]
  (some-> (table-file tname id)
    load-edn))


(defn get-all
  "Reads and returns the contents of the given dir."
  [tname]
  (some->> (table-file tname)
    fs/list-dir
    (map fs/name)
    (map #(get-by-id tname %))))


(defn select
  "Returns a list of records that match the given key/value pairs.
  Opts is a map of #{:limit :order-by :offset :filter}.
   
   (select 
     :users 
     {:where #(= (:name %) \"John\"),
      :offset 10, :limit 10, :order-by :age})}))"
  ([tname opts]
   (let [records (get-all tname)
         records (if-let [where (:where opts)]
                   (filter where records)
                   records)
         records (if-let [k (:order-by opts)]
                   (sort-by k records)
                   records)
         records (if (= :desc (:order-by opts))
                   (reverse records)
                   records)
         records (if-let [offset (:offset opts)]
                   (drop offset records)
                   records)
         records (if-let [limit (:limit opts)]
                   (take limit records)
                   records)]
     records)))


(defn get-by
  "Returns the first record that matches the given key/value pairs.
  Opts is a map of #{:order-by :filter}.
   
   (get-by 
     :users 
     {:name \"John\"}
     {:order-by :age})}"
  ([tname data]
   (get-by tname data nil))
  ([tname data opts]
   (first (select tname (merge {:where #(= data %)} opts)))))


(defn create!
  "Creates a new table record. Returns the data with the id."
  [tname data]
  ; We generate the next id for the table and assoc it to the data map before
  ; adding the data to the db.
  (let [id (next-id! tname)
        data (assoc data
               (get-record-key tname :id)
               id)]
    (save-edn! (io/file
                 (table-path tname)
                 (str id))
      data)))

(defn create-raw!
  "Creates a new table record. Unlike `create!`, the user must provide the
  id. Returns the data with the id."
  [tname data]
  ;; check if there's a `id` key
  (assert (contains? data (get-record-key tname :id))
    (str
      "You must provide an `id` key, or use `create!` to have it automatically "
      "generated."))
  
  (let [id (get data
             (get-record-key tname :id))]
    (save-edn! (io/file
                 (table-path tname)
                 (str id))
      data)))
  

(defn hard-update!
  "Updates the record for the given table id, replacing all its value for the
   value of `data`. If a record can't be found, returns nil."
  [tname data]
  (let [id (get data
             (get-record-key tname :id))
        file (table-file tname id)]
    (when file
      (save-edn! file data))))


(defn update!
  "Updates the record for the given table id, only for the keys given in `data`.
   If a record can't be found, returns nil."
  [tname data]
  (let [id (get data
             (if (:use-qualified-keywords? @settings)
               (keyword (name tname) "id")
               :id))
        file (table-file tname id)
        old-data (get-by-id tname id)]
    (when file
      (save-edn! file
        (merge old-data data)))))


(defn delete!
  "Deletes the record at the given file. If successful returns true. If the
  file doesn't exist, returns false."
  [tname id]
  (some-> (table-file tname id)
    fs/delete))



(comment
  
  "tests:"

  (delete-table! :user)
  (create-table! :user)
  
  ; turn on qualified keywords (it's false by default)
  (swap! settings assoc :use-qualified-keywords? true)

  (get-by-id :user 1)
  (get-all :user)
  (create! :user {:user/name "Guest"})
  
  (create-table! :profile)
  (create-raw! :profile {:profile/id "Guest" :dob "2023-01-01"})
  (get-by-id :profile "Guest")
  
  (update! :user {:user/id 1 :user/name "Guest" :user/age 18})
  
  ; removing keys from db:
  (hard-update! :user {:user/id 1 :user/name "Guest"})
  
  (delete! :user 1))