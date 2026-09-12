;; Adapted from Datalevin JOB-bench, EPL-2.0; see NOTICE.md.
(ns job.source
  (:require [clojure.string :as s])
  (:import [job CSVReader]))

(defn- csv-rows [reader]
  (let [rows (CSVReader. reader)]
    (reify Iterable (iterator [_] rows))))

(def schema
  {:aka-name/person        {:db/valueType :db.type/ref}
   :aka-name/name          {:db/valueType :db.type/string}
   :aka-name/imdb-index    {:db/valueType :db.type/string}
   :aka-name/name-pcode-cf {:db/valueType :db.type/string}
   :aka-name/name-pcode-nf {:db/valueType :db.type/string}
   :aka-name/surname-pcode {:db/valueType :db.type/string}

   :aka-title/movie           {:db/valueType :db.type/ref}
   :aka-title/title           {:db/valueType :db.type/string}
   :aka-title/imdb-index      {:db/valueType :db.type/string}
   :aka-title/kind            {:db/valueType :db.type/ref}
   :aka-title/production-year {:db/valueType :db.type/long}
   :aka-title/phonetic-code   {:db/valueType :db.type/string}
   :aka-title/episode-of      {:db/valueType :db.type/ref}
   :aka-title/season-nr       {:db/valueType :db.type/long}
   :aka-title/episode-nr      {:db/valueType :db.type/long}
   :aka-title/note            {:db/valueType :db.type/string}

   :cast-info/person      {:db/valueType :db.type/ref}
   :cast-info/movie       {:db/valueType :db.type/ref}
   :cast-info/person-role {:db/valueType :db.type/ref}
   :cast-info/note        {:db/valueType :db.type/string}
   :cast-info/nr-order    {:db/valueType :db.type/long}
   :cast-info/role        {:db/valueType :db.type/ref}

   :char-name/name          {:db/valueType :db.type/string}
   :char-name/imdb-index    {:db/valueType :db.type/string}
   :char-name/imdb-id       {:db/valueType :db.type/long}
   :char-name/name-pcode-nf {:db/valueType :db.type/string}
   :char-name/surname-pcode {:db/valueType :db.type/string}

   :comp-cast-type/kind {:db/valueType :db.type/string}

   :company-name/name          {:db/valueType :db.type/string}
   :company-name/country-code  {:db/valueType :db.type/string}
   :company-name/imdb-id       {:db/valueType :db.type/long}
   :company-name/name-pcode-nf {:db/valueType :db.type/string}
   :company-name/name-pcode-sf {:db/valueType :db.type/string}

   :company-type/kind {:db/valueType :db.type/string}

   :complete-cast/movie   {:db/valueType :db.type/ref}
   :complete-cast/subject {:db/valueType :db.type/ref}
   :complete-cast/status  {:db/valueType :db.type/ref}

   :info-type/info {:db/valueType :db.type/string}

   :keyword/keyword       {:db/valueType :db.type/string}
   :keyword/phonetic-code {:db/valueType :db.type/string}

   :kind-type/kind {:db/valueType :db.type/string}

   :link-type/link {:db/valueType :db.type/string}

   :movie-companies/movie        {:db/valueType :db.type/ref}
   :movie-companies/company      {:db/valueType :db.type/ref}
   :movie-companies/company-type {:db/valueType :db.type/ref}
   :movie-companies/note         {:db/valueType :db.type/string}

   :movie-info/movie     {:db/valueType :db.type/ref}
   :movie-info/info-type {:db/valueType :db.type/ref}
   :movie-info/info      {:db/valueType :db.type/string}
   :movie-info/note      {:db/valueType :db.type/string}

   :movie-info-idx/movie     {:db/valueType :db.type/ref}
   :movie-info-idx/info-type {:db/valueType :db.type/ref}
   :movie-info-idx/info      {:db/valueType :db.type/string}
   :movie-info-idx/note      {:db/valueType :db.type/string}

   :movie-keyword/movie   {:db/valueType :db.type/ref}
   :movie-keyword/keyword {:db/valueType :db.type/ref}

   :movie-link/movie        {:db/valueType :db.type/ref}
   :movie-link/linked-movie {:db/valueType :db.type/ref}
   :movie-link/link-type    {:db/valueType :db.type/ref}

   :name/name          {:db/valueType :db.type/string}
   :name/imdb-index    {:db/valueType :db.type/string}
   :name/imdb-id       {:db/valueType :db.type/long}
   :name/gender        {:db/valueType :db.type/string}
   :name/name-pcode-cf {:db/valueType :db.type/string}
   :name/name-pcode-nf {:db/valueType :db.type/string}
   :name/surname-pcode {:db/valueType :db.type/string}

   :person-info/person    {:db/valueType :db.type/ref}
   :person-info/info-type {:db/valueType :db.type/ref}
   :person-info/info      {:db/valueType :db.type/string}
   :person-info/note      {:db/valueType :db.type/string}

   :role-type/role {:db/valueType :db.type/string}

   :title/title           {:db/valueType :db.type/string}
   :title/imdb-index      {:db/valueType :db.type/string}
   :title/kind            {:db/valueType :db.type/ref}
   :title/production-year {:db/valueType :db.type/long}
   :title/imdb-id         {:db/valueType :db.type/long}
   :title/phonetic-code   {:db/valueType :db.type/string}
   :title/episode-of      {:db/valueType :db.type/ref}
   :title/season-nr       {:db/valueType :db.type/long}
   :title/episode-nr      {:db/valueType :db.type/long}
   :title/series-years    {:db/valueType :db.type/string}})

;; loading the data

;; eid base
(def aka-name-base        1000000)
(def aka-title-base       2000000)
(def cast-info-base       100000000)
(def char-name-base       10000000)
(def comp-cast-type-base  0)
(def company-name-base    3000000)
(def company-type-base    10)
(def complete-cast-base   4000000)
(def info-type-base       1000)
(def keyword-base         5000000)
(def kind-type-base       20)
(def link-type-base       30)
(def movie-companies-base 20000000)
(def movie-info-base      30000000)
(def movie-info-idx-base  40000000)
(def movie-keyword-base   50000000)
(def movie-link-base      100000)
(def name-base            60000000)
(def person-info-base     70000000)
(def role-type-base       50)
(def title-base           80000000)

(defn- add-comp-cast-type [reader]
  (eduction
    (map (fn [[id content]]
           (vector (+ ^long comp-cast-type-base (Long/parseLong id))
                    :comp-cast-type/kind content)))
    (csv-rows reader)))

(defn- add-company-type [reader]
  (eduction
    (map (fn [[id content]]
           (vector (+ ^long company-type-base (Long/parseLong id))
                    :company-type/kind content)))
    (csv-rows reader)))

(defn- add-kind-type [reader]
  (eduction
    (map (fn [[id content]]
           (vector (+ ^long kind-type-base (Long/parseLong id))
                    :kind-type/kind content)))
    (csv-rows reader)))

(defn- add-link-type [reader]
  (eduction
    (map (fn [[id content]]
           (vector (+ ^long link-type-base (Long/parseLong id))
                    :link-type/link content)))
    (csv-rows reader)))

(defn- add-role-type [reader]
  (eduction
    (map (fn [[id content]]
           (vector (+ ^long role-type-base (Long/parseLong id))
                    :role-type/role content)))
    (csv-rows reader)))

(defn- add-info-type [reader]
  (eduction
    (map (fn [[id content]]
           (vector (+ ^long info-type-base (Long/parseLong id))
                    :info-type/info content)))
    (csv-rows reader)))

(defn- add-movie-link [reader]
  (eduction
    (comp
      (map (fn [[id movie linked-movie link-type]]
             (let [eid (+ ^long movie-link-base (Long/parseLong id))]
               [(vector eid :movie-link/movie
                         (+ ^long title-base (Long/parseLong movie)))
                (vector eid :movie-link/linked-movie
                         (+ ^long title-base (Long/parseLong linked-movie)))
                (vector eid :movie-link/link-type
                         (+ ^long link-type-base (Long/parseLong link-type)))])))
      cat)
    (csv-rows reader)))

(defn- add-aka-name [reader]
  (eduction
    (comp
      (map
        (fn [[id person name imdb-index name-pcode-cf
             name-pcode-nf surname-pcode]]
          (let [eid (+ ^long aka-name-base (Long/parseLong id))]
            (cond-> [(vector eid :aka-name/person
                              (+ ^long name-base (Long/parseLong person)))
                     (vector eid :aka-name/name name)]
              (not (s/blank? imdb-index))
              (conj (vector eid :aka-name/imdb-index imdb-index))
              (not (s/blank? name-pcode-cf))
              (conj (vector eid :aka-name/name-pcode-cf name-pcode-cf))
              (not (s/blank? name-pcode-nf))
              (conj (vector eid :aka-name/name-pcode-nf name-pcode-nf))
              (not (s/blank? surname-pcode))
              (conj (vector eid :aka-name/surname-pcode surname-pcode))))))
      cat)
    (csv-rows reader)))

(defn- add-aka-title [reader]
  (eduction
    (comp
      (map
        (fn [[id movie title imdb-index kind production-year phonetic-code
             episode-of season-nr episode-nr note]]
          (let [eid (+ ^long aka-title-base (Long/parseLong id))]
            (cond-> [(vector eid :aka-title/movie
                              (+ ^long title-base (Long/parseLong movie)))
                     (vector eid :aka-title/title title)]
              (not (s/blank? imdb-index))
              (conj (vector eid :aka-title/imdb-index imdb-index))
              (not (s/blank? kind))
              (conj (vector eid :aka-title/kind
                             (+ ^long kind-type-base (Long/parseLong kind))))
              (not (s/blank? production-year))
              (conj (vector eid :aka-title/production-year
                             (Long/parseLong production-year)))
              (not (s/blank? phonetic-code))
              (conj (vector eid :aka-title/phonetic-code phonetic-code))
              (not (s/blank? episode-of))
              (conj (vector eid :aka-title/episode-of
                             (+ ^long title-base (Long/parseLong episode-of))))
              (not (s/blank? season-nr))
              (conj (vector eid :aka-title/season-nr (Long/parseLong season-nr)))
              (not (s/blank? episode-nr))
              (conj (vector eid :aka-title/episode-nr (Long/parseLong episode-nr)))
              (not (s/blank? note))
              (conj (vector eid :aka-title/note note))))))
      cat)
    (csv-rows reader)))

(defn- add-company-name [reader]
  (eduction
    (comp
      (map
        (fn [[id name country-code imdb-id name-pcode-nf name-pcode-sf]]
          (let [eid (+ ^long company-name-base (Long/parseLong id))]
            (cond-> [(vector eid :company-name/name name)]
              (not (s/blank? country-code))
              (conj (vector eid :company-name/country-code country-code))
              (not (s/blank? imdb-id))
              (conj (vector eid :company-name/imdb-id (Long/parseLong imdb-id)))
              (not (s/blank? name-pcode-nf))
              (conj (vector eid :company-name/name-pcode-nf name-pcode-nf))
              (not (s/blank? name-pcode-sf))
              (conj (vector eid :company-name/name-pcode-sf name-pcode-sf))))))
      cat)
    (csv-rows reader)))

(defn- add-complete-cast [reader]
  (eduction
    (comp
      (map
        (fn [[id movie subject status]]
          (let [eid (+ ^long complete-cast-base (Long/parseLong id))]
            [(vector eid :complete-cast/movie
                      (+ ^long title-base (Long/parseLong movie)))
             (vector eid :complete-cast/subject
                      (+ ^long comp-cast-type-base (Long/parseLong subject)))
             (vector eid :complete-cast/status
                      (+ ^long comp-cast-type-base (Long/parseLong status)))])))
      cat)
    (csv-rows reader)))

(defn- add-keyword [reader]
  (eduction
    (comp
      (map
        (fn [[id keyword phonetic-code]]
          (let [eid (+ ^long keyword-base (Long/parseLong id))]
            [(vector eid :keyword/keyword keyword)
             (vector eid :keyword/phonetic-code phonetic-code)])))
      cat)
    (csv-rows reader)))

(defn- add-char-name [reader]
  (eduction
    (comp
      (map
        (fn [[id name imdb-index imdb-id name-pcode-nf surname-pcode]]
          (let [eid (+ ^long char-name-base (Long/parseLong id))]
            (cond-> [(vector eid :char-name/name name)]
              (not (s/blank? imdb-index))
              (conj (vector eid :char-name/imdb-index imdb-index))
              (not (s/blank? imdb-id))
              (conj (vector eid :char-name/imdb-id (Long/parseLong imdb-id)))
              (not (s/blank? name-pcode-nf))
              (conj (vector eid :char-name/name-pcode-nf name-pcode-nf))
              (not (s/blank? surname-pcode))
              (conj (vector eid :char-name/surname-pcode surname-pcode))))))
      cat)
    (csv-rows reader)))

(defn- add-movie-companies [reader]
  (eduction
    (comp
      (map
        (fn [[id movie company company-type note]]
          (let [eid (+ ^long movie-companies-base (Long/parseLong id))]
            (cond-> [(vector eid :movie-companies/movie
                              (+ ^long title-base (Long/parseLong movie)))
                     (vector eid :movie-companies/company
                              (+ ^long company-name-base
                                 (Long/parseLong company)))
                     (vector eid :movie-companies/company-type
                              (+ ^long company-type-base
                                 (Long/parseLong company-type)))]
              (not (s/blank? note))
              (conj (vector eid :movie-companies/note note))))))
      cat)
    (csv-rows reader)))

(defn- add-movie-info [reader]
  (eduction
    (comp
      (map (fn [[id movie info-type info note]]
             (let [eid (+ ^long movie-info-base (Long/parseLong id))]
               (cond-> [(vector eid :movie-info/movie
                                 (+ ^long title-base (Long/parseLong movie)))
                        (vector eid :movie-info/info-type
                                 (+ ^long info-type-base
                                    (Long/parseLong info-type)))
                        (vector eid :movie-info/info info)]
                 (not (s/blank? note))
                 (conj (vector eid :movie-info/note note))))))
      cat)
    (csv-rows reader)))

(defn- add-movie-info-idx [reader]
  (eduction
    (comp
      (map
        (fn [[id movie info-type info note]]
          (let [eid (+ ^long movie-info-idx-base (Long/parseLong id))]
            (cond-> [(vector eid :movie-info-idx/movie
                              (+ ^long title-base (Long/parseLong movie)))
                     (vector eid :movie-info-idx/info-type
                              (+ ^long info-type-base (Long/parseLong info-type)))
                     (vector eid :movie-info-idx/info info)]
              (not (s/blank? note))
              (conj (vector eid :movie-info-idx/note note))))))
      cat)
    (csv-rows reader)))

(defn- add-movie-keyword [reader]
  (eduction
    (comp
      (map (fn [[id movie keyword]]
             (let [eid (+ ^long movie-keyword-base (Long/parseLong id))]
               [(vector eid :movie-keyword/movie
                         (+ ^long title-base (Long/parseLong movie)))
                (vector eid :movie-keyword/keyword
                         (+ ^long keyword-base (Long/parseLong keyword)))])))
      cat)
    (csv-rows reader)))

(defn- add-name [reader]
  (eduction
    (comp
      (map
        (fn [[id name imdb-index imdb-id gender name-pcode-cf name-pcode-nf
             surname-pcode]]
          (let [eid (+ ^long name-base (Long/parseLong id))]
            (cond-> [(vector eid :name/name name)]
              (not (s/blank? imdb-index))
              (conj (vector eid :name/imdb-index imdb-index))
              (not (s/blank? imdb-id))
              (conj (vector eid :name/imdb-id (Long/parseLong imdb-id)))
              (not (s/blank? gender))
              (conj (vector eid :name/gender gender))
              (not (s/blank? name-pcode-cf))
              (conj (vector eid :name/name-pcode-cf name-pcode-cf))
              (not (s/blank? name-pcode-nf))
              (conj (vector eid :name/name-pcode-nf name-pcode-nf))
              (not (s/blank? surname-pcode))
              (conj (vector eid :name/surname-pcode surname-pcode))))))
      cat)
    (csv-rows reader)))

(defn- add-person-info [reader]
  (eduction
    (comp
      (map
        (fn [[id person info-type info note]]
          (let [eid (+ ^long person-info-base (Long/parseLong id))]
            (cond-> [(vector eid :person-info/person
                              (+ ^long name-base (Long/parseLong person)))
                     (vector eid :person-info/info-type
                              (+ ^long info-type-base (Long/parseLong info-type)))
                     (vector eid :person-info/info info)]
              (not (s/blank? note))
              (conj (vector eid :person-info/note note))))))
      cat)
    (csv-rows reader)))

(defn- add-title [reader]
  (eduction
    (comp
      (map
        (fn [[id title imdb-index kind production-year imdb-id phonetic-code
             episode-of season-nr episode-nr series-years]]
          (let [eid (+ ^long title-base (Long/parseLong id))]
            (cond-> [(vector eid :title/title title)
                     (vector eid :title/kind
                              (+ ^long kind-type-base (Long/parseLong kind)))]
              (not (s/blank? imdb-index))
              (conj (vector eid :title/imdb-index imdb-index))
              (not (s/blank? production-year))
              (conj (vector eid :title/production-year
                             (Long/parseLong production-year)))
              (not (s/blank? imdb-id))
              (conj (vector eid :title/imdb-id (Long/parseLong imdb-id)))
              (not (s/blank? phonetic-code))
              (conj (vector eid :title/phonetic-code phonetic-code))
              (not (s/blank? episode-of))
              (conj (vector eid :title/episode-of
                             (+ ^long title-base (Long/parseLong episode-of))))
              (not (s/blank? season-nr))
              (conj (vector eid :title/season-nr (Long/parseLong season-nr)))
              (not (s/blank? episode-nr))
              (conj (vector eid :title/episode-nr
                             (Long/parseLong episode-nr)))
              (not (s/blank? series-years))
              (conj (vector eid :title/series-years series-years))))))
      cat)
    (csv-rows reader)))

(defn- add-cast-info [reader]
  (eduction
    (comp
      (map
        (fn [[id person movie person-role note nr-order role]]
          (let [eid (+ ^long cast-info-base (Long/parseLong id))]
            (cond-> [(vector eid :cast-info/person
                              (+ ^long name-base (Long/parseLong person)))
                     (vector eid :cast-info/movie
                              (+ ^long title-base (Long/parseLong movie)))
                     (vector eid :cast-info/role
                              (+ ^long role-type-base (Long/parseLong role)))]
              (not (s/blank? person-role))
              (conj (vector eid :cast-info/person-role
                             (+ ^long char-name-base (Long/parseLong person-role))))
              (not (s/blank? note))
              (conj (vector eid :cast-info/note note))
              (not (s/blank? nr-order))
              (conj (vector eid :cast-info/nr-order

                             (Long/parseLong nr-order)))))))
      cat)
    (csv-rows reader)))


(def tables
  [[:comp-cast-type add-comp-cast-type]
   [:company-type add-company-type]
   [:kind-type add-kind-type]
   [:link-type add-link-type]
   [:role-type add-role-type]
   [:info-type add-info-type]
   [:movie-link add-movie-link]
   [:aka-name add-aka-name]
   [:aka-title add-aka-title]
   [:company-name add-company-name]
   [:complete-cast add-complete-cast]
   [:keyword add-keyword]
   [:char-name add-char-name]
   [:movie-companies add-movie-companies]
   [:movie-info add-movie-info]
   [:movie-info-idx add-movie-info-idx]
   [:movie-keyword add-movie-keyword]
   [:name add-name]
   [:person-info add-person-info]
   [:title add-title]
   [:cast-info add-cast-info]])
