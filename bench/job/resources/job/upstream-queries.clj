(def q-1a '[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
            :where
            [?ct :company-type/kind "production companies"]
            [?it :info-type/info "top 250 rank"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
            [(or (like ?mc.note "%(co-production)%")
                 (like ?mc.note "%(presents)%"))]
            [?mc :movie-companies/company-type ?ct]
            [?mc :movie-companies/movie ?t]
            [?mi :movie-info-idx/movie ?t]
            [?mi :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]
            [?t :title/production-year ?t.production-year]
            ])

;; good plan
(def q-1b '[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
            :where
            [?ct :company-type/kind "production companies"]
            [?it :info-type/info "bottom 10 rank"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
            [?t :title/production-year ?t.production-year]
            [(<= 2005 ?t.production-year 2010)]
            [?mc :movie-companies/company-type ?ct]
            [?mc :movie-companies/movie ?t]
            [?mi :movie-info-idx/movie ?t]
            [?mi :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

(def q-1c '[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
            :where
            [?ct :company-type/kind "production companies"]
            [?it :info-type/info "top 250 rank"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
            [(like ?mc.note "%(co-production)%")]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mc :movie-companies/company-type ?ct]
            [?mc :movie-companies/movie ?t]
            [?mi :movie-info-idx/movie ?t]
            [?mi :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

;; good plan
(def q-1d '[:find (min ?mc.note) (min ?t.title) (min ?t.production-year)
            :where
            [?ct :company-type/kind "production companies"]
            [?it :info-type/info "bottom 10 rank"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(as Metro-Goldwyn-Mayer Pictures)%")]
            [?t :title/production-year ?t.production-year]
            [(< 2000 ?t.production-year)]
            [?mc :movie-companies/company-type ?ct]
            [?mc :movie-companies/movie ?t]
            [?mi :movie-info-idx/movie ?t]
            [?mi :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

;; good plan
(def q-2a '[:find (min ?t.title)
            :where
            [?cn :company-name/country-code "[de]"]
            [?k :keyword/keyword "character-name-in-title"]
            [?mc :movie-companies/company ?cn]
            [?mc :movie-companies/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

;; good plan
(def q-2b '[:find (min ?t.title)
            :where
            [?cn :company-name/country-code "[nl]"]
            [?k :keyword/keyword "character-name-in-title"]
            [?mc :movie-companies/company ?cn]
            [?mc :movie-companies/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

;; good plan
(def q-2c '[:find (min ?t.title)
            :where
            [?cn :company-name/country-code "[sm]"]
            [?k :keyword/keyword "character-name-in-title"]
            [?mc :movie-companies/company ?cn]
            [?mc :movie-companies/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

;; good plan
(def q-2d '[:find (min ?t.title)
            :where
            [?cn :company-name/country-code "[us]"]
            [?k :keyword/keyword "character-name-in-title"]
            [?mc :movie-companies/company ?cn]
            [?mc :movie-companies/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

(def q-3a '[:find (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                           "Denish", "Norwegian", "German"])]
            [?t :title/production-year ?t.production-year]
            [(< 2005 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

;; good plan
(def q-3b '[:find (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Bulgaria"])]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

(def q-3c '[:find (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                           "Denish", "Norwegian", "German", "USA", "American"])]
            [?t :title/production-year ?t.production-year]
            [(< 1990 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?t :title/title ?t.title]])

(def q-4a '[:find (min ?mi-idx.info) (min ?t.title)
            :where
            [?it :info-type/info "rating"]
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi-idx :movie-info-idx/info ?mi-idx.info]
            [(< "5.0" ?mi-idx.info)]
            [?t :title/production-year ?t.production-year]
            [(< 2005 ?t.production-year)]
            [?mi-idx :movie-info-idx/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?mi-idx :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

(def q-4b '[:find (min ?mi-idx.info) (min ?t.title)
            :where
            [?it :info-type/info "rating"]
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi-idx :movie-info-idx/info ?mi-idx.info]
            [(< "9.0" ?mi-idx.info)]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mi-idx :movie-info-idx/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?mi-idx :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

(def q-4c '[:find (min ?mi-idx.info) (min ?t.title)
            :where
            [?it :info-type/info "rating"]
            [?k :keyword/keyword ?k.keyword]
            [(like ?k.keyword "%sequel%")]
            [?mi-idx :movie-info-idx/info ?mi-idx.info]
            [(< "2.0" ?mi-idx.info)]
            [?t :title/production-year ?t.production-year]
            [(< 1990 ?t.production-year)]
            [?mi-idx :movie-info-idx/movie ?t]
            [?mk :movie-keyword/movie ?t]
            [?mk :movie-keyword/keyword ?k]
            [?mi-idx :movie-info-idx/info-type ?it]
            [?t :title/title ?t.title]])

(def q-5a '[:find (min ?t.title)
            :where
            [?ct :company-type/kind "production companies"]
            [?mc :movie-companies/note ?mc.note]
            [(like ?mc.note "%(theatrical)%")]
            [(like ?mc.note "%(France)%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                           "Denish", "Norwegian", "German"])]
            [?t :title/production-year ?t.production-year]
            [(< 2005 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company-type ?ct]
            [?t :title/title ?t.title]])

(def q-5b '[:find (min ?t.title)
            :where
            [?ct :company-type/kind "production companies"]
            [?mc :movie-companies/note ?mc.note]
            [(like ?mc.note "%(VHS)%")]
            [(like ?mc.note "%(USA)%")]
            [(like ?mc.note "%(1994)%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["USA" "America"])]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company-type ?ct]
            [?t :title/title ?t.title]])

(def q-5c '[:find (min ?t.title)
            :where
            [?ct :company-type/kind "production companies"]
            [?mc :movie-companies/note ?mc.note]
            [(not-like ?mc.note "%(TV)%")]
            [(like ?mc.note "%(USA)%")]
            [?mi :movie-info/info ?mi.info]
            [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                           "Denish", "Norwegian", "German", "USA", "American"])]
            [?t :title/production-year ?t.production-year]
            [(< 1990 ?t.production-year)]
            [?mi :movie-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company-type ?ct]
            [?t :title/title ?t.title]])

;; good plan
(def q-6a '[:find (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword "marvel-cinematic-universe"]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2010 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-6b '[:find (min ?k.keyword) (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(in ?k.keyword ["superhero", "sequel", "second-part", "marvel-comics",
                             "based-on-comic", "tv-special", "fight", "violence"])]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2014 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-6c '[:find (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword "marvel-cinematic-universe"]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2014 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-6d '[:find (min ?k.keyword) (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(in ?k.keyword ["superhero", "sequel", "second-part", "marvel-comics",
                             "based-on-comic", "tv-special", "fight", "violence"])]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2000 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-6e '[:find (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword "marvel-cinematic-universe"]
            [?n :name/name ?n.name]
            [(like ?n.name "%Downey%Robert%")]
            [?t :title/production-year ?t.production-year]
            [(< 2000 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-6f '[:find (min ?k.keyword) (min ?n.name) (min ?t.title)
            :where
            [?k :keyword/keyword ?k.keyword]
            [(in ?k.keyword ["superhero", "sequel", "second-part", "marvel-comics",
                             "based-on-comic", "tv-special", "fight", "violence"])]
            [?n :name/name ?n.name]
            [?t :title/production-year ?t.production-year]
            [(< 2000 ?t.production-year)]
            [?mk :movie-keyword/keyword ?k]
            [?mk :movie-keyword/movie ?t]
            [?ci :cast-info/movie ?t]
            [?ci :cast-info/person ?n]
            [?t :title/title ?t.title]])

;; good plan
(def q-7a '[:find (min ?n.name) (min ?t.title)
            :where
            [?an :aka-name/name ?an.name]
            [(like ?an.name "%a%")]
            [?it :info-type/info "mini biography"]
            [?lt :link-type/link "features"]
            [?n :name/name-pcode-cf ?n.name-pcode-cf]
            [(<= "A" ?n.name-pcode-cf "F")]
            [?n :name/gender ?n.gender]
            [?n :name/name ?n.name]
            [(or (= ?n.gender "m") (and (= ?n.gender "f") (like ?n.name "B%")))]
            [?pi :person-info/note "Volker Boehm"]
            [?t :title/production-year ?t.production-year]
            [(<= 1980 ?t.production-year 1995)]
            [?an :aka-name/person ?n]
            [?pi :person-info/person ?n]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/movie ?t]
            [?ml :movie-link/linked-movie ?t]
            [?ml :movie-link/link-type ?lt]
            [?pi :person-info/info-type ?it]
            [?t :title/title ?t.title]])

;; good plan
(def q-7b '[:find (min ?n.name) (min ?t.title)
            :where
            [?an :aka-name/name ?an.name]
            [(like ?an.name "%a%")]
            [?it :info-type/info "mini biography"]
            [?lt :link-type/link "features"]
            [?n :name/name-pcode-cf ?n.name-pcode-cf]
            [(like ?n.name-pcode-cf "D%")]
            [?n :name/gender "m"]
            [?n :name/name ?n.name]
            [?pi :person-info/note "Volker Boehm"]
            [?t :title/production-year ?t.production-year]
            [(<= 1980 ?t.production-year 1984)]
            [?an :aka-name/person ?n]
            [?pi :person-info/person ?n]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/movie ?t]
            [?ml :movie-link/linked-movie ?t]
            [?ml :movie-link/link-type ?lt]
            [?pi :person-info/info-type ?it]
            [?t :title/title ?t.title]])

;; good plan
(def q-7c '[:find (min ?n.name) (min ?pi.info)
            :where
            [?an :aka-name/name ?an.name]
            [(or (like ?an.name "%a%") (like ?an.name "A%"))]
            [?it :info-type/info "mini biography"]
            [?lt :link-type/link ?lt.link]
            [(in ?lt.link ["references", "referenced in", "features",
                           "featured in"])]
            [?n :name/name-pcode-cf ?n.name-pcode-cf]
            [(<= "A" ?n.name-pcode-cf "F")]
            [?n :name/gender ?n.gender]
            [?n :name/name ?n.name]
            [(or (= ?n.gender "m") (and (= ?n.gender "f") (like ?n.name "A%")))]
            [?pi :person-info/note _]
            [?pi :person-info/info ?pi.info]
            [?t :title/production-year ?t.production-year]
            [(<= 1980 ?t.production-year 2010)]
            [?an :aka-name/person ?n]
            [?pi :person-info/person ?n]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/movie ?t]
            [?ml :movie-link/linked-movie ?t]
            [?ml :movie-link/link-type ?lt]
            [?pi :person-info/info-type ?it]])

(def q-8a '[:find (min ?an1.name) (min ?t.title)
            :where
            [?ci :cast-info/note "(voice: English version)"]
            [?cn :company-name/country-code "[jp]"]
            [?mc :movie-companies/note ?mc.note]
            [(like ?mc.note "%(Japan)%")]
            [(not-like ?mc.note "%(USA)%")]
            [?n1 :name/name ?n1.name]
            [(like ?n1.name "%Yo%")]
            [(not-like ?n1.name "%Yu%")]
            [?rt :role-type/role "actress"]
            [?an1 :aka-name/person ?n1]
            [?an1 :aka-name/name ?an1.name]
            [?ci :cast-info/person ?n1]
            [?ci :cast-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company ?cn]
            [?ci :cast-info/role ?rt]
            [?t :title/title ?t.title]])

(def q-8b '[:find (min ?an.name) (min ?t.title)
            :where
            [?ci :cast-info/note "(voice: English version)"]
            [?cn :company-name/country-code "[jp]"]
            [?mc :movie-companies/note ?mc.note]
            [(like ?mc.note "%(Japan)%")]
            [(not-like ?mc.note "%(USA)%")]
            [(or (like ?mc.note "%(2006)%") (like ?mc.note "%(2007)%"))]
            [?n :name/name ?n.name]
            [(like ?n.name "%Yo%")]
            [(not-like ?n.name "%Yu%")]
            [?rt :role-type/role "actress"]
            [?t :title/production-year ?t.production-year]
            [(<= 2006 ?t.production-year 2007)]
            [?t :title/title ?t.title]
            [(or (like ?t.title "One Piece%") (like ?t.title "Dragon Ball Z%"))]
            [?an :aka-name/person ?n]
            [?an :aka-name/name ?an.name]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company ?cn]
            [?ci :cast-info/role ?rt]])

;; good plan
(def q-8c '[:find (min ?an.name) (min ?t.title)
            :where
            [?cn :company-name/country-code "[us]"]
            [?rt :role-type/role "writer"]
            [?an :aka-name/person ?n]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company ?cn]
            [?ci :cast-info/role ?rt]
            [?t :title/title ?t.title]
            [?an :aka-name/name ?an.name]])

;; good plan
(def q-8d '[:find (min ?an.name) (min ?t.title)
            :where
            [?cn :company-name/country-code "[us]"]
            [?rt :role-type/role "costume designer"]
            [?an :aka-name/person ?n]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company ?cn]
            [?ci :cast-info/role ?rt]
            [?t :title/title ?t.title]
            [?an :aka-name/name ?an.name]])

(def q-9a '[:find (min ?an.name) (min ?chn.name) (min ?t.title)
            :where
            [?ci :cast-info/note ?ci.note]
            [(in ?ci.note ["(voice)", "(voice: Japanese version)",
                           "(voice) (uncredited)", "(voice: English version)"])]
            [?cn :company-name/country-code "[us]"]
            [?mc :movie-companies/note ?mc.note]
            [(or (like ?mc.note "%(USA)%") (like ?mc.note "%(worldwide)%"))]
            [?n :name/gender "f"]
            [?n :name/name ?n.name]
            [(like ?n.name "%Ang%")]
            [?rt :role-type/role "actress"]
            [?t :title/production-year ?t.production-year]
            [(<= 2005 ?t.production-year 2015)]
            [?ci :cast-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company ?cn]
            [?ci :cast-info/role ?rt]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/person-role ?chn]
            [?chn :char-name/name ?chn.name]
            [?an :aka-name/person ?n]
            [?t :title/title ?t.title]
            [?an :aka-name/name ?an.name]])

(def q-9b '[:find (min ?an.name) (min ?chn.name) (min ?n.name) (min ?t.title)
            :where
            [?ci :cast-info/note "(voice)"]
            [?cn :company-name/country-code "[us]"]
            [?mc :movie-companies/note ?mc.note]
            [(like ?mc.note "%(200%)%")]
            [(or (like ?mc.note "%(USA)%") (like ?mc.note "%(worldwide)%"))]
            [?n :name/gender "f"]
            [?n :name/name ?n.name]
            [(like ?n.name "%Ang%")]
            [?rt :role-type/role "actress"]
            [?t :title/production-year ?t.production-year]
            [(<= 2007 ?t.production-year 2010)]
            [?ci :cast-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company ?cn]
            [?ci :cast-info/role ?rt]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/person-role ?chn]
            [?chn :char-name/name ?chn.name]
            [?an :aka-name/person ?n]
            [?t :title/title ?t.title]
            [?an :aka-name/name ?an.name]])

(def q-9c '[:find (min ?an.name) (min ?chn.name) (min ?n.name) (min ?t.title)
            :where
            [?ci :cast-info/note ?ci.note]
            [(in ?ci.note ["(voice)", "(voice: Japanese version)",
                           "(voice) (uncredited)", "(voice: English version)"])]
            [?cn :company-name/country-code "[us]"]
            [?n :name/gender "f"]
            [?n :name/name ?n.name]
            [(like ?n.name "%An%")]
            [?rt :role-type/role "actress"]
            [?ci :cast-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company ?cn]
            [?ci :cast-info/role ?rt]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/person-role ?chn]
            [?chn :char-name/name ?chn.name]
            [?an :aka-name/person ?n]
            [?t :title/title ?t.title]
            [?an :aka-name/name ?an.name]])

(def q-9d '[:find (min ?an.name) (min ?chn.name) (min ?n.name) (min ?t.title)
            :where
            [?ci :cast-info/note ?ci.note]
            [(in ?ci.note ["(voice)", "(voice: Japanese version)",
                           "(voice) (uncredited)", "(voice: English version)"])]
            [?cn :company-name/country-code "[us]"]
            [?n :name/gender "f"]
            [?n :name/name ?n.name]
            [?rt :role-type/role "actress"]
            [?ci :cast-info/movie ?t]
            [?mc :movie-companies/movie ?t]
            [?mc :movie-companies/company ?cn]
            [?ci :cast-info/role ?rt]
            [?ci :cast-info/person ?n]
            [?ci :cast-info/person-role ?chn]
            [?chn :char-name/name ?chn.name]
            [?an :aka-name/person ?n]
            [?t :title/title ?t.title]
            [?an :aka-name/name ?an.name]])

(def q-10a '[:find (min ?chn.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(like ?ci.note "%(voice)%")]
             [(like ?ci.note "%(uncredited)%")]
             [?cn :company-name/country-code "[ru]"]
             [?rt :role-type/role "actor"]
             [?t :title/production-year ?t.production-year]
             [(< 2005 ?t.production-year)]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?ci :cast-info/person-role ?chn]
             [?ci :cast-info/role ?rt]
             [?mc :movie-companies/company ?cn]
             [?chn :char-name/name ?chn.name]
             [?t :title/title ?t.title]])

;; good plan
(def q-10b '[:find (min ?chn.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(like ?ci.note "%(producer)%")]
             [?cn :company-name/country-code "[ru]"]
             [?rt :role-type/role "actor"]
             [?t :title/production-year ?t.production-year]
             [(< 2010 ?t.production-year)]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?ci :cast-info/person-role ?chn]
             [?ci :cast-info/role ?rt]
             [?mc :movie-companies/company ?cn]
             [?chn :char-name/name ?chn.name]
             [?t :title/title ?t.title]])

;; bad
(def q-10c '[:find (min ?chn.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(like ?ci.note "%(producer)%")]
             [?cn :company-name/country-code "[us]"]
             [?t :title/production-year ?t.production-year]
             [(< 1990 ?t.production-year)]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?ci :cast-info/person-role ?chn]
             [?mc :movie-companies/company ?cn]
             [?chn :char-name/name ?chn.name]
             [?t :title/title ?t.title]])

;; good plan
(def q-11a '[:find (min ?cn.name) (min ?lt.link) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [(or (like ?cn.name "%Film%") (like ?cn.name "%Warner%"))]
             [?ct :company-type/kind "production companies"]
             [?k :keyword/keyword "sequel"]
             [?lt :link-type/link ?lt.link]
             [(like ?lt.link "%follow%")]
             [(missing? $ ?mc :movie-companies/note)]
             [?t :title/production-year ?t.production-year]
             [(<= 1950 ?t.production-year 2000)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]])

;; good plan
(def q-11b '[:find (min ?cn.name) (min ?lt.link) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [(or (like ?cn.name "%Film%") (like ?cn.name "%Warner%"))]
             [?ct :company-type/kind "production companies"]
             [?k :keyword/keyword "sequel"]
             [?lt :link-type/link ?lt.link]
             [(like ?lt.link "%follows%")]
             [(missing? $ ?mc :movie-companies/note)]
             [?t :title/production-year 1998]
             [?t :title/title ?t.title]
             [(like ?t.title "%Money%")]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]])

(def q-11c '[:find (min ?cn.name) (min ?mc.note) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [(or (like ?cn.name "20th Century Fox%")
                  (like ?cn.name "Twentieth Century Fox%"))]
             [?ct :company-type/kind ?ct.kind]
             [(not= ?ct.kind "production companies")]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["sequel" "revenge" "based-on-novel"])]
             [?mc :movie-companies/note ?mc.note]
             [?t :title/production-year ?t.production-year]
             [(< 1950 ?t.production-year)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]])

;; good plan
(def q-11d '[:find (min ?cn.name) (min ?mc.note) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [?ct :company-type/kind ?ct.kind]
             [(not= ?ct.kind "production companies")]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["sequel" "revenge" "based-on-novel"])]
             [?mc :movie-companies/note ?mc.note]
             [?t :title/production-year ?t.production-year]
             [(< 1950 ?t.production-year)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]])

(def q-12a '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?cn :company-name/name ?cn.name]
             [?ct :company-type/kind  "production companies"]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "rating"]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Drama" "Horror"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< "8.0" ?mi-idx.info)]
             [?t :title/production-year ?t.production-year]
             [(<= 2005 ?t.production-year 2008)]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]])

(def q-12b '[:find (min ?mi.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?ct :company-type/kind ?ct.kind]
             [(in ?ct.kind ["production companies" "distributors"])]
             [?it1 :info-type/info "budget"]
             [?it2 :info-type/info "bottom 10 rank"]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/title ?t.title]
             [(or (like ?t.title "Birdemic%") (like ?t.title "%Movie%"))]
             [?mi :movie-info/info ?mi.info]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]])

(def q-12c '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?cn :company-name/name ?cn.name]
             [?ct :company-type/kind "production companies"]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "rating"]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Drama" "Horror" "Western" "Family"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< "7.0" ?mi-idx.info)]
             [?t :title/production-year ?t.production-year]
             [(<= 2000 ?t.production-year 2010)]
             [?t :title/title ?t.title]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]])

(def q-13a '[:find (min ?mi.info) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[de]"]
             [?ct :company-type/kind "production companies"]
             [?it1 :info-type/info "rating"]
             [?it2 :info-type/info "release dates"]
             [?kt :kind-type/kind "movie"]
             [?mi :movie-info/movie ?t]
             [?mi :movie-info/info-type ?it2]
             [?t :title/kind ?kt]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mc :movie-companies/company-type ?ct]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mi-idx :movie-info-idx/info-type ?it1]
             [?mi :movie-info/info ?mi.info]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [?t :title/title ?t.title]])

(def q-13b '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?cn :company-name/name ?cn.name]
             [?ct :company-type/kind "production companies"]
             [?it1 :info-type/info "rating"]
             [?it2 :info-type/info "release dates"]
             [?kt :kind-type/kind "movie"]
             [?t :title/title ?t.title]
             [(not= ?t.title "")]
             [(or (like ?t.title "%Champion%") (like ?t.title "%Loser%"))]
             [?mi :movie-info/movie ?t]
             [?mi :movie-info/info-type ?it2]
             [?t :title/kind ?kt]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mc :movie-companies/company-type ?ct]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mi-idx :movie-info-idx/info-type ?it1]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

(def q-13c '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?cn :company-name/name ?cn.name]
             [?ct :company-type/kind "production companies"]
             [?it1 :info-type/info "rating"]
             [?it2 :info-type/info "release dates"]
             [?kt :kind-type/kind "movie"]
             [?t :title/title ?t.title]
             [(not= ?t.title "")]
             [(or (like ?t.title "Champion%") (like ?t.title "Loser%"))]
             [?mi :movie-info/movie ?t]
             [?mi :movie-info/info-type ?it2]
             [?t :title/kind ?kt]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mc :movie-companies/company-type ?ct]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mi-idx :movie-info-idx/info-type ?it1]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

(def q-13d '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?cn :company-name/name ?cn.name]
             [?ct :company-type/kind "production companies"]
             [?it1 :info-type/info "rating"]
             [?it2 :info-type/info "release dates"]
             [?kt :kind-type/kind "movie"]
             [?t :title/title ?t.title]
             [?mi :movie-info/movie ?t]
             [?mi :movie-info/info-type ?it2]
             [?t :title/kind ?kt]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mc :movie-companies/company-type ?ct]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mi-idx :movie-info-idx/info-type ?it1]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

(def q-14a '[:find (min ?mi-idx.info) (min ?t.title)
             :where
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title", "blood", "violence"])]
             [?kt :kind-type/kind "movie"]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Denish", "Norwegian", "German", "USA", "American"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< ?mi-idx.info "8.5")]
             [?t :title/production-year ?t.production-year]
             [(< 2010 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?t :title/title ?t.title]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             ])

(def q-14b '[:find (min ?mi-idx.info) (min ?t.title)
             :where
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title"])]
             [?kt :kind-type/kind "movie"]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Denish", "Norwegian", "German", "USA", "American"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< "6.0" ?mi-idx.info)]
             [?t :title/production-year ?t.production-year]
             [(< 2010 ?t.production-year)]
             [?t :title/title ?t.title]
             [(or (like ?t.title "%murder%") (like ?t.title "%Murder%")
                  (like ?t.title "%Mord%"))]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             ])

(def q-14c '[:find (min ?mi-idx.info) (min ?t.title)
             :where
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title", "blood", "violence"])]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie" "episode"])]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Denish", "Norwegian", "German", "USA", "American"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< ?mi-idx.info "8.5")]
             [?t :title/production-year ?t.production-year]
             [(< 2005 ?t.production-year)]
             [?t :title/title ?t.title]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             ])

(def q-15a '[:find (min ?mi.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?it1 :info-type/info "release dates"]
             [?mc :movie-companies/note ?mc.note]
             [(like ?mc.note "%(200%)%")]
             [(like ?mc.note "%(worldwide)%")]
             [?mi :movie-info/note ?mi.note]
             [(like ?mi.note "%internet%")]
             [?mi :movie-info/info ?mi.info]
             [(and (like ?mi.info "USA:%") (like ?mi.info "% 200%"))]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?at :aka-title/movie ?t]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]
             [?mc :movie-companies/company-type ?ct]
             ])

;; good plan
(def q-15b '[:find (min ?mi.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?cn :company-name/name "YouTube"]
             [?it1 :info-type/info "release dates"]
             [?mc :movie-companies/note ?mc.note]
             [(like ?mc.note "%(200%)%")]
             [(like ?mc.note "%(worldwide)%")]
             [?mi :movie-info/note ?mi.note]
             [(like ?mi.note "%internet%")]
             [?mi :movie-info/info ?mi.info]
             [(and (like ?mi.info "USA:%") (like ?mi.info "% 200%"))]
             [?t :title/production-year ?t.production-year]
             [(<= 2005 ?t.production-year 2010)]
             [?at :aka-title/movie ?t]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]
             [?mc :movie-companies/company-type ?ct]
             ])

;; bad
(def q-15c '[:find (min ?mi.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?it1 :info-type/info "release dates"]
             [?mi :movie-info/note ?mi.note]
             [(like ?mi.note "%internet%")]
             [?mi :movie-info/info ?mi.info]
             [(and (like ?mi.info "USA:%")
                   (or (like ?mi.info "% 199%") (like ?mi.info "% 200%")))]
             [?t :title/production-year ?t.production-year]
             [(< 1990 ?t.production-year)]
             [?at :aka-title/movie ?t]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]
             [?mc :movie-companies/company-type ?ct]
             ])

(def q-15d '[:find (min ?mi.info) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?it1 :info-type/info "release dates"]
             [?mi :movie-info/note ?mi.note]
             [(like ?mi.note "%internet%")]
             [?mi :movie-info/info ?mi.info]
             [?t :title/production-year ?t.production-year]
             [(< 1990 ?t.production-year)]
             [?at :aka-title/movie ?t]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mi :movie-info/info-type ?it1]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]
             [?mc :movie-companies/company-type ?ct]
             [?mk :movie-keyword/movie ?t]
             ])

;; good plan
(def q-16a '[:find (min ?an.name) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?k :keyword/keyword "character-name-in-title"]
             [?t :title/episode-nr ?t.episode-nr]
             [(<= 50 ?t.episode-nr)]
             [(< ?t.episode-nr 100)]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?an :aka-name/name ?an.name]
             [?t :title/title ?t.title]
             ])

;; good plan
(def q-16b '[:find (min ?an.name) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?k :keyword/keyword "character-name-in-title"]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?an :aka-name/name ?an.name]
             [?t :title/title ?t.title]
             ])

(def q-16c '[:find (min ?an.name) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?k :keyword/keyword "character-name-in-title"]
             [?t :title/episode-nr ?t.episode-nr]
             [(< ?t.episode-nr 100) ]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?an :aka-name/name ?an.name]
             [?t :title/title ?t.title]
             ])

;; good plan
(def q-16d '[:find (min ?an.name) (min ?t.title)
             :where
             [?cn :company-name/country-code "[us]"]
             [?k :keyword/keyword "character-name-in-title"]
             [?t :title/episode-nr ?t.episode-nr]
             [(<= 5 ?t.episode-nr)]
             [(< ?t.episode-nr 100)]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?an :aka-name/name ?an.name]
             [?t :title/title ?t.title]
             ])

;; good plan
(def q-17a '[:find (min ?n.name)
             :where
             [?cn :company-name/country-code "[us]"]
             [?k :keyword/keyword "character-name-in-title"]
             [?n :name/name ?n.name]
             [(like ?n.name "B%")]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             ])

;; good plan
(def q-17b '[:find (min ?n.name)
             :where
             [?k :keyword/keyword "character-name-in-title"]
             [?n :name/name ?n.name]
             [(like ?n.name "Z%")]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             ])

;; good plan
(def q-17c '[:find (min ?n.name)
             :where
             [?k :keyword/keyword "character-name-in-title"]
             [?n :name/name ?n.name]
             [(like ?n.name "X%")]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             ])

;; good plan
(def q-17d '[:find (min ?n.name)
             :where
             [?k :keyword/keyword "character-name-in-title"]
             [?n :name/name ?n.name]
             [(like ?n.name "%Bert%")]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             ])

;; good plan
(def q-17e '[:find (min ?n.name)
             :where
             [?cn :company-name/country-code "[us]"]
             [?k :keyword/keyword "character-name-in-title"]
             [?n :name/name ?n.name]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             ])

;; good plan
(def q-17f '[:find (min ?n.name)
             :where
             [?k :keyword/keyword "character-name-in-title"]
             [?n :name/name ?n.name]
             [(like ?n.name "%B%")]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company ?cn]
             ])

(def q-18a '[:find (min ?mi.info) (min ?mi-idx.info) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(producer)", "(executive producer)"])]
             [?it1 :info-type/info "budget"]
             [?it2 :info-type/info "votes"]
             [?n :name/gender "m"]
             [?n :name/name ?n.name]
             [(like ?n.name "%Tim%")]
             [?mi :movie-info/movie ?t]
             [?mi :movie-info/info ?mi.info]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [?ci :cast-info/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?t :title/title ?t.title]
             ])

(def q-18b '[:find (min ?mi.info) (min ?mi-idx.info) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)" "(written by)"
                            "(story)" "(story editor)"])]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "rating"]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Horror" "Thriller"])]
             [(missing? $ ?mi :movie-info/note)]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< "8.0" ?mi-idx.info)]
             [?n :name/gender "f"]
             [?t :title/production-year ?t.production-year]
             [(<= 2008 ?t.production-year 2014)]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?t :title/title ?t.title]
             ])

;; good plan
(def q-18c '[:find (min ?mi.info) (min ?mi-idx.info) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)" "(written by)"
                            "(story)" "(story editor)"])]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Horror" "Action" "Sci-Fi" "Thriller"
                            "Crime" "War"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [?n :name/gender "m"]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?t :title/title ?t.title]
             ])

(def q-19a '[:find (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(voice)", "(voice: Japanese version)"
                            "(voice) (uncredited)"
                            "(voice: English version)"])]
             [?cn :company-name/country-code "[us]"]
             [?it :info-type/info "release dates"]
             [?mc :movie-companies/note ?mc.note]
             [(or (like ?mc.note "%(USA)%") (like ?mc.note "%(worldwide)%"))]
             [?mi :movie-info/info ?mi.info]
             [(and (or (like ?mi.info "Japan:%") (like ?mi.info "USA:%"))
                   (like ?mi.info "%200%"))]
             [?n :name/gender "f"]
             [?n :name/name ?n.name]
             [(like ?n.name "%Ang%")]
             [?rt :role-type/role "actress"]
             [?t :title/production-year ?t.production-year]
             [(<= 2005 ?t.production-year 2009)]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/info-type ?it]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/role ?rt]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person-role ?chn]
             [?t :title/title ?t.title]
             ])

(def q-19b '[:find (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note "(voice)"]
             [?cn :company-name/country-code "[us]"]
             [?it :info-type/info "release dates"]
             [?mc :movie-companies/note ?mc.note]
             [(like ?mc.note "%(200%)%")]
             [(or (like ?mc.note "%(USA)%") (like ?mc.note "%(worldwide)%"))]
             [?mi :movie-info/info ?mi.info]
             [(or (like ?mi.info "Japan:%2007%") (like ?mi.info "USA:%2008%"))]
             [?n :name/gender "f"]
             [?n :name/name ?n.name]
             [(like ?n.name "%Angel%")]
             [?rt :role-type/role "actress"]
             [?t :title/production-year ?t.production-year]
             [(<= 2007 ?t.production-year 2008)]
             [?t :title/title ?t.title]
             [(like ?t.title "%Kung%Fu%Panda%")]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/info-type ?it]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/role ?rt]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person-role ?chn]
             ])

;; good plan
(def q-19c '[:find (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(voice)", "(voice: Japanese version)"
                            "(voice) (uncredited)"
                            "(voice: English version)"])]
             [?cn :company-name/country-code "[us]"]
             [?it :info-type/info "release dates"]
             [?mi :movie-info/info ?mi.info]
             [(and (or (like ?mi.info "Japan:%") (like ?mi.info "USA:%"))
                   (like ?mi.info "%200%"))]
             [?n :name/gender "f"]
             [?n :name/name ?n.name]
             [(like ?n.name "%An%")]
             [?rt :role-type/role "actress"]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/title ?t.title]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/info-type ?it]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/role ?rt]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person-role ?chn]
             ])

(def q-19d '[:find (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(voice)", "(voice: Japanese version)"
                            "(voice) (uncredited)"
                            "(voice: English version)"])]
             [?cn :company-name/country-code "[us]"]
             [?it :info-type/info "release dates"]
             [?n :name/gender "f"]
             [?n :name/name ?n.name]
             [?rt :role-type/role "actress"]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/title ?t.title]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/info-type ?it]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/role ?rt]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person-role ?chn]
             ])

;; good plan
(def q-20a '[:find (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind ?cct2.kind]
             [(like ?cct2.kind "%complete%")]
             [?chn :char-name/name ?chn.name]
             [(not-like ?chn.name "%Sherlock%")]
             [(or (like ?chn.name "%Tony%Stark%") (like ?chn.name "%Iron%Man%"))]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["superhero", "sequel", "second-part",
                              "marvel-comics", "based-on-comic",
                              "tv-special", "fight", "violence"])]
             [?kt :kind-type/kind "movie"]
             [?t :title/production-year ?t.production-year]
             [(< 1950 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mk :movie-keyword/movie ?t]
             [?ci :cast-info/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?ci :cast-info/person-role ?chn]
             [?ci :cast-info/person ?n]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?t :title/title ?t.title]])

;; good plan
(def q-20b '[:find (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind ?cct2.kind]
             [(like ?cct2.kind "%complete%")]
             [?chn :char-name/name ?chn.name]
             [(not-like ?chn.name "%Sherlock%")]
             [(or (like ?chn.name "%Tony%Stark%") (like ?chn.name "%Iron%Man%"))]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["superhero", "sequel", "second-part",
                              "marvel-comics", "based-on-comic",
                              "tv-special", "fight", "violence"])]
             [?kt :kind-type/kind "movie"]
             [?n :name/name ?n.name]
             [(like ?n.name "%Downey%Robert%")]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mk :movie-keyword/movie ?t]
             [?ci :cast-info/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?ci :cast-info/person-role ?chn]
             [?ci :cast-info/person ?n]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?t :title/title ?t.title]])

;; good plan
(def q-20c '[:find (min ?n.name) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind ?cct2.kind]
             [(like ?cct2.kind "%complete%")]
             [?chn :char-name/name ?chn.name]
             [(or (like ?chn.name "%man%") (like ?chn.name "%Man%"))]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["superhero", "marvel-comics", "based-on-comic",
                              "tv-special", "fight", "violence", "magnet",
                              "web", "claw", "laser"])]
             [?kt :kind-type/kind "movie"]
             [?n :name/name ?n.name]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mk :movie-keyword/movie ?t]
             [?ci :cast-info/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?ci :cast-info/person-role ?chn]
             [?ci :cast-info/person ?n]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?t :title/title ?t.title]])

;; good plan
(def q-21a '[:find (min ?cn.name) (min ?lt.link) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [(or (like ?cn.name "%Film%") (like ?cn.name "%Warner%"))]
             [?ct :company-type/kind "production companies"]
             [?k :keyword/keyword "sequel"]
             [?lt :link-type/link ?lt.link]
             [(like ?lt.link "%follow%")]
             [(missing? $ ?mc :movie-companies/note)]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Denish", "Norwegian", "German"])]
             [?t :title/production-year ?t.production-year]
             [(<= 1950 ?t.production-year 2000)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/movie ?t]
             [?t :title/title ?t.title]])

(def q-21b '[:find (min ?cn.name) (min ?lt.link) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [(or (like ?cn.name "%Film%") (like ?cn.name "%Warner%"))]
             [?ct :company-type/kind "production companies"]
             [?k :keyword/keyword "sequel"]
             [?lt :link-type/link ?lt.link]
             [(like ?lt.link "%follow%")]
             [(missing? $ ?mc :movie-companies/note)]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Germany", "German"])]
             [?t :title/production-year ?t.production-year]
             [(<= 2000 ?t.production-year 2010)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/movie ?t]
             [?t :title/title ?t.title]])

(def q-21c '[:find (min ?cn.name) (min ?lt.link) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [(or (like ?cn.name "%Film%") (like ?cn.name "%Warner%"))]
             [?ct :company-type/kind "production companies"]
             [?k :keyword/keyword "sequel"]
             [?lt :link-type/link ?lt.link]
             [(like ?lt.link "%follow%")]
             [(missing? $ ?mc :movie-companies/note)]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Denish", "Norwegian", "German", "English"])]
             [?t :title/production-year ?t.production-year]
             [(<= 1950 ?t.production-year 2010)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/movie ?t]
             [?t :title/title ?t.title]])

(def q-22a '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[us]")]
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title", "blood", "violence"])]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie", "episode"])]
             [?mc :movie-companies/note ?mc.note]
             [(not-like ?mc.note "%(USA)%")]
             [(like ?mc.note "%(200%)%")]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Germany","German", "USA", "American"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< ?mi-idx.info "7.0")]
             [?t :title/production-year ?t.production-year]
             [(< 2008 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]
             [?cn :company-name/name ?cn.name]
             ])

(def q-22b '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[us]")]
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title", "blood", "violence"])]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie", "episode"])]
             [?mc :movie-companies/note ?mc.note]
             [(not-like ?mc.note "%(USA)%")]
             [(like ?mc.note "%(200%)%")]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Germany","German", "USA", "American"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< ?mi-idx.info "7.0")]
             [?t :title/production-year ?t.production-year]
             [(< 2009 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]
             [?cn :company-name/name ?cn.name]
             ])

(def q-22c '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[us]")]
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title", "blood", "violence"])]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie", "episode"])]
             [?mc :movie-companies/note ?mc.note]
             [(not-like ?mc.note "%(USA)%")]
             [(like ?mc.note "%(200%)%")]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Danish", "Norwegian", "German", "USA", "American"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< ?mi-idx.info "8.5")]
             [?t :title/production-year ?t.production-year]
             [(< 2005 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]
             [?cn :company-name/name ?cn.name]
             ])

(def q-22d '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[us]")]
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title", "blood", "violence"])]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie", "episode"])]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Danish", "Norwegian", "German", "USA", "American"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< ?mi-idx.info "8.5")]
             [?t :title/production-year ?t.production-year]
             [(< 2005 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?t :title/title ?t.title]
             [?cn :company-name/name ?cn.name]
             ])

(def q-23a '[:find (min ?kt.kind) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "complete+verified"]
             [?cn :company-name/country-code "[us]"]
             [?it1 :info-type/info "release dates"]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie"])]
             [?mi :movie-info/note ?mi.note]
             [(like ?mi.note "%internet%")]
             [?mi :movie-info/info ?mi.info]
             [(and (or (like ?mi.info "% 199%") (like ?mi.info "% 200%"))
                   (like ?mi.info "USA:%"))]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mc :movie-companies/company ?cn]
             [?mc :movie-companies/company-type ?ct]
             [?cc :complete-cast/status ?cct1]
             [?t :title/title ?t.title]
             ])

(def q-23b '[:find (min ?kt.kind) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "complete+verified"]
             [?cn :company-name/country-code "[us]"]
             [?it1 :info-type/info "release dates"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["nerd", "loner", "alienation", "dignity"])]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie"])]
             [?mi :movie-info/note ?mi.note]
             [(like ?mi.note "%internet%")]
             [?mi :movie-info/info ?mi.info]
             [(and (like ?mi.info "% 200%") (like ?mi.info "USA:%"))]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mc :movie-companies/company ?cn]
             [?mc :movie-companies/company-type ?ct]
             [?cc :complete-cast/status ?cct1]
             [?t :title/title ?t.title]
             ])

(def q-23c '[:find (min ?kt.kind) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "complete+verified"]
             [?cn :company-name/country-code "[us]"]
             [?it1 :info-type/info "release dates"]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie", "tv movie", "video movie", "video game"])]
             [?mi :movie-info/note ?mi.note]
             [(like ?mi.note "%internet%")]
             [?mi :movie-info/info ?mi.info]
             [(and (or (like ?mi.info "% 199%") (like ?mi.info "% 200%"))
                   (like ?mi.info "USA:%"))]
             [?t :title/production-year ?t.production-year]
             [(< 1990 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mc :movie-companies/company ?cn]
             [?mc :movie-companies/company-type ?ct]
             [?cc :complete-cast/status ?cct1]
             [?t :title/title ?t.title]
             ])

(def q-24a '[:find (min ?chn.name) (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(voice)", "(voice: Japanese version)",
                            "(voice) (uncredited)", "(voice: English version)"])]
             [?cn :company-name/country-code "[us]"]
             [?it :info-type/info "release dates"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["hero", "martial-arts", "hand-to-hand-combat"])]
             [?mi :movie-info/info ?mi.info]
             [(and (or (like ?mi.info "Japan:%") (like ?mi.info "USA:%"))
                   (like ?mi.info "%201%"))]
             [?n :name/gender "f"]
             [?n :name/name ?n.name]
             [(like ?n.name "%An%")]
             [?rt :role-type/role "actress"]
             [?t :title/production-year ?t.production-year]
             [(< 2010 ?t.production-year)]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/info-type ?it]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/role ?rt]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person-role ?chn]
             [?chn :char-name/name ?chn.name]
             [?mk :movie-keyword/keyword ?k]
             [?t :title/title ?t.title]
             ])

;; good plan
(def q-24b '[:find (min ?chn.name) (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(voice)", "(voice: Japanese version)",
                            "(voice) (uncredited)", "(voice: English version)"])]
             [?cn :company-name/country-code "[us]"]
             [?cn :company-name/name "DreamWorks Animation"]
             [?it :info-type/info "release dates"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["hero", "martial-arts", "hand-to-hand-combat",
                              "computer-animated-movie"])]
             [?mi :movie-info/info ?mi.info]
             [(and (or (like ?mi.info "Japan:%") (like ?mi.info "USA:%"))
                   (like ?mi.info "%201%"))]
             [?n :name/gender "f"]
             [?n :name/name ?n.name]
             [(like ?n.name "%An%")]
             [?rt :role-type/role "actress"]
             [?t :title/production-year ?t.production-year]
             [(< 2010 ?t.production-year)]
             [?t :title/title ?t.title]
             [(like ?t.title "Kung Fu Panda%")]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/info-type ?it]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/role ?rt]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person-role ?chn]
             [?chn :char-name/name ?chn.name]
             [?mk :movie-keyword/keyword ?k]
             ])

(def q-25a '[:find (min ?mi-idx.info) (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)", "(written by)",
                            "(story)", "(story editor)"])]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "blood", "gore", "death",
                              "female-nudity"])]
             [?mi :movie-info/info "Horror"]
             [?n :name/gender "m"]
             [?n :name/name ?n.name]
             [?t :title/title ?t.title]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mk :movie-keyword/keyword ?k]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

;; good plan
(def q-25b '[:find (min ?mi-idx.info) (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)", "(written by)",
                            "(story)", "(story editor)"])]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "blood", "gore", "death",
                              "female-nudity"])]
             [?mi :movie-info/info "Horror"]
             [?n :name/gender "m"]
             [?n :name/name ?n.name]
             [?t :title/production-year ?t.production-year]
             [(< 2010 ?t.production-year)]
             [?t :title/title ?t.title]
             [(like ?t.title "Vampire%")]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mk :movie-keyword/keyword ?k]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

(def q-25c '[:find (min ?mi.info) (min ?mi-idx.info) (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)", "(written by)",
                            "(story)", "(story editor)"])]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "violence", "blood", "gore", "death",
                              "female-nudity", "hospital"])]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Horror", "Action", "Sci-Fi", "Thriller", "Crime",
                            "War"])]
             [?n :name/gender "m"]
             [?n :name/name ?n.name]
             [?t :title/title ?t.title]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mk :movie-keyword/keyword ?k]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

;; good plan
(def q-26a '[:find (min ?chn.name) (min ?mi-idx.info) (min ?n.name)
             (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind ?cct2.kind]
             [(like ?cct2.kind "%complete%")]
             [?chn :char-name/name ?chn.name]
             [(or (like ?chn.name "%man%") (like ?chn.name "%Man%"))]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["superhero", "marvel-comics", "based-on-comic",
                              "tv-special", "fight", "violence", "magnet",
                              "web", "claw", "laser"])]
             [?kt :kind-type/kind "movie"]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< "7.0" ?mi-idx.info)]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mk :movie-keyword/movie ?t]
             [?ci :cast-info/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/person-role ?chn]
             [?ci :cast-info/person ?n]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?n :name/name ?n.name]
             [?t :title/title ?t.title]
             ])

;; good plan
(def q-26b '[:find (min ?chn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind ?cct2.kind]
             [(like ?cct2.kind "%complete%")]
             [?chn :char-name/name ?chn.name]
             [(or (like ?chn.name "%man%") (like ?chn.name "%Man%"))]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["superhero", "marvel-comics", "based-on-comic",
                              "fight"])]
             [?kt :kind-type/kind "movie"]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< "8.0" ?mi-idx.info)]
             [?t :title/production-year ?t.production-year]
             [(< 2005 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mk :movie-keyword/movie ?t]
             [?ci :cast-info/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/person-role ?chn]
             [?ci :cast-info/person ?n]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?t :title/title ?t.title]
             ])

;; good plan
(def q-26c '[:find (min ?chn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind ?cct2.kind]
             [(like ?cct2.kind "%complete%")]
             [?chn :char-name/name ?chn.name]
             [(or (like ?chn.name "%man%") (like ?chn.name "%Man%"))]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["superhero", "marvel-comics", "based-on-comic",
                              "tv-special", "fight", "violence", "magnet",
                              "web", "claw", "laser"])]
             [?kt :kind-type/kind "movie"]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mk :movie-keyword/movie ?t]
             [?ci :cast-info/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/person-role ?chn]
             [?ci :cast-info/person ?n]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?t :title/title ?t.title]
             ])

(def q-27a '[:find (min ?cn.name) (min ?lt.link) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind ?cct1.kind]
             [(in ?cct1.kind ["cast", "crew"])]
             [?cct2 :comp-cast-type/kind "complete"]
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [(or (like ?cn.name "%Film%") (like ?cn.name "%Warner%"))]
             [?ct :company-type/kind "production companies"]
             [?k :keyword/keyword "sequel"]
             [?lt :link-type/link ?lt.link]
             [(like ?lt.link "%follow%")]
             [(missing? $ ?mc :movie-companies/note)]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Germany", "Swedish", "German"])]
             [?t :title/production-year ?t.production-year]
             [(<= 1950 ?t.production-year 2000)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?t :title/title ?t.title]
             ])

(def q-27b '[:find (min ?cn.name) (min ?lt.link) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind ?cct1.kind]
             [(in ?cct1.kind ["cast", "crew"])]
             [?cct2 :comp-cast-type/kind "complete"]
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [(or (like ?cn.name "%Film%") (like ?cn.name "%Warner%"))]
             [?ct :company-type/kind "production companies"]
             [?k :keyword/keyword "sequel"]
             [?lt :link-type/link ?lt.link]
             [(like ?lt.link "%follow%")]
             [(missing? $ ?mc :movie-companies/note)]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Germany", "Swedish", "German"])]
             [?t :title/production-year 1998]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?t :title/title ?t.title]
             ])

(def q-27c '[:find (min ?cn.name) (min ?lt.link) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind ?cct2.kind]
             [(like ?cct2.kind "complete%")]
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[pl]")]
             [?cn :company-name/name ?cn.name]
             [(or (like ?cn.name "%Film%") (like ?cn.name "%Warner%"))]
             [?ct :company-type/kind "production companies"]
             [?k :keyword/keyword "sequel"]
             [?lt :link-type/link ?lt.link]
             [(like ?lt.link "%follow%")]
             [(missing? $ ?mc :movie-companies/note)]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Denish", "Norwegian", "German", "English"])]
             [?t :title/production-year ?t.production-year]
             [(<= 1950 ?t.production-year 2010)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/movie ?t]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?t :title/title ?t.title]
             ])

(def q-28a '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "crew"]
             [?cct2 :comp-cast-type/kind ?cct2.kind]
             [(not= ?cct2.kind "complete+verified")]
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[us]")]
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title", "blood", "violence"])]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie", "episode"])]
             [?mc :movie-companies/note ?mc.note]
             [(not-like ?mc.note "%(USA)%")]
             [(like ?mc.note "%(200%)%")]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Danish", "Norwegian", "German", "USA", "American"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< ?mi-idx.info "8.5")]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?cn :company-name/name ?cn.name]
             [?t :title/title ?t.title]
             ])

(def q-28b '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "crew"]
             [?cct2 :comp-cast-type/kind ?cct2.kind]
             [(not= ?cct2.kind "complete+verified")]
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[us]")]
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title", "blood", "violence"])]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie", "episode"])]
             [?mc :movie-companies/note ?mc.note]
             [(not-like ?mc.note "%(USA)%")]
             [(like ?mc.note "%(200%)%")]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Germany", "Swedish", "German"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< "6.5" ?mi-idx.info)]
             [?t :title/production-year ?t.production-year]
             [(< 2005 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?cn :company-name/name ?cn.name]
             [?t :title/title ?t.title]
             ])

;; good plan
(def q-28c '[:find (min ?cn.name) (min ?mi-idx.info) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind "complete"]
             [?cn :company-name/country-code ?cn.country-code]
             [(not= ?cn.country-code "[us]")]
             [?it1 :info-type/info "countries"]
             [?it2 :info-type/info "rating"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "murder-in-title", "blood", "violence"])]
             [?kt :kind-type/kind ?kt.kind]
             [(in ?kt.kind ["movie", "episode"])]
             [?mc :movie-companies/note ?mc.note]
             [(not-like ?mc.note "%(USA)%")]
             [(like ?mc.note "%(200%)%")]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Sweden", "Norway", "Germany", "Denmark", "Swedish",
                            "Danish", "Norwegian", "German", "USA", "American"])]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [(< ?mi-idx.info "8.5")]
             [?t :title/production-year ?t.production-year]
             [(< 2005 ?t.production-year)]
             [?t :title/kind ?kt]
             [?mi :movie-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mk :movie-keyword/keyword ?k]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mc :movie-companies/company-type ?ct]
             [?mc :movie-companies/company ?cn]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?cn :company-name/name ?cn.name]
             [?t :title/title ?t.title]
             ])

;; bad
(def q-29a '[:find (min ?n.name)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind "complete+verified"]
             [?chn :char-name/name "Queen"]
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(voice)", "(voice) (uncredited)",
                            "(voice: English version)"])]
             [?cn :company-name/country-code "[us]"]
             [?it :info-type/info "release dates"]
             [?it3 :info-type/info "trivia"]
             [?k :keyword/keyword "computer-animation"]
             [?mi :movie-info/info ?mi.info]
             [(and (or (like ?mi.info "Japan:%") (like ?mi.info "USA:%"))
                   (like ?mi.info "%200%"))]
             [?n :name/gender "f"]
             [?n :name/name ?n.name]
             [(like ?n.name "%An%")]
             [?rt :role-type/role "actress"]
             [?t :title/title "Shrek 2"]
             [?t :title/production-year ?t.production-year]
             [(<= 2000 ?t.production-year 2010)]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/info-type ?it]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/role ?rt]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person-role ?chn]
             [?pi :person-info/person ?n]
             [?pi :person-info/info-type ?it3]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             ])

(def q-29b '[:find (min ?n.name)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind "complete+verified"]
             [?chn :char-name/name "Queen"]
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(voice)", "(voice) (uncredited)",
                            "(voice: English version)"])]
             [?cn :company-name/country-code "[us]"]
             [?it :info-type/info "release dates"]
             [?it3 :info-type/info "height"]
             [?k :keyword/keyword "computer-animation"]
             [?mi :movie-info/info ?mi.info]
             [(and (like ?mi.info "USA:%") (like ?mi.info "%200%"))]
             [?n :name/gender "f"]
             [?n :name/name ?n.name]
             [(like ?n.name "%An%")]
             [?rt :role-type/role "actress"]
             [?t :title/title "Shrek 2"]
             [?t :title/production-year ?t.production-year]
             [(<= 2000 ?t.production-year 2005)]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/info-type ?it]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/role ?rt]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person-role ?chn]
             [?pi :person-info/person ?n]
             [?pi :person-info/info-type ?it3]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             ])

(def q-29c '[:find (min ?chn.name) (min ?n.name) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind "complete+verified"]
             [?chn :char-name/name ?chn.name]
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(voice)", "(voice: Japanese version)",
                            "(voice) (uncredited)", "(voice: English version)"])]
             [?cn :company-name/country-code "[us]"]
             [?it :info-type/info "release dates"]
             [?it3 :info-type/info "trivia"]
             [?k :keyword/keyword "computer-animation"]
             [?mi :movie-info/info ?mi.info]
             [(and (or (like ?mi.info "Japan:%") (like ?mi.info "USA:%"))
                   (like ?mi.info "%200%"))]
             [?n :name/gender "f"]
             [?n :name/name ?n.name]
             [(like ?n.name "%An%")]
             [?rt :role-type/role "actress"]
             [?t :title/title ?t.title]
             [?t :title/production-year ?t.production-year]
             [(<= 2000 ?t.production-year 2010)]
             [?mi :movie-info/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?mc :movie-companies/company ?cn]
             [?mi :movie-info/info-type ?it]
             [?ci :cast-info/person ?n]
             [?ci :cast-info/role ?rt]
             [?an :aka-name/person ?n]
             [?ci :cast-info/person-role ?chn]
             [?pi :person-info/person ?n]
             [?pi :person-info/info-type ?it3]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             ])

(def q-30a '[:find (min ?mi.info) (min ?mi-idx.info) (min ?n.name) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind ?cct1.kind]
             [(in ?cct1.kind ["cast" "crew"])]
             [?cct2 :comp-cast-type/kind "complete+verified"]
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)", "(written by)",
                            "(story)", "(story editor)"])]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "violence", "blood", "gore", "death",
                              "female-nudity", "hospital"])]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Horror", "Thriller"])]
             [?n :name/gender "m"]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?n :name/name ?n.name]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             [?t :title/title ?t.title]
             ])

(def q-30b '[:find (min ?mi.info) (min ?mi-idx.info) (min ?n.name) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind ?cct1.kind]
             [(in ?cct1.kind ["cast" "crew"])]
             [?cct2 :comp-cast-type/kind "complete+verified"]
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)", "(written by)",
                            "(story)", "(story editor)"])]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "violence", "blood", "gore", "death",
                              "female-nudity", "hospital"])]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Horror", "Thriller"])]
             [?n :name/gender "m"]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/title ?t.title]
             [(or (like ?t.title "%Freddy%") (like ?t.title "%Jason%")
                  (like ?t.title "Saw%"))]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?n :name/name ?n.name]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

(def q-30c '[:find (min ?mi.info) (min ?mi-idx.info) (min ?n.name) (min ?t.title)
             :where
             [?cct1 :comp-cast-type/kind "cast"]
             [?cct2 :comp-cast-type/kind "complete+verified"]
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)", "(written by)",
                            "(story)", "(story editor)"])]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "violence", "blood", "gore", "death",
                              "female-nudity", "hospital"])]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Horror", "Action", "Sci-Fi", "Thriller" "Crime",
                            "War"])]
             [?n :name/gender "m"]
             [?t :title/title ?t.title]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?cc :complete-cast/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mk :movie-keyword/keyword ?k]
             [?cc :complete-cast/subject ?cct1]
             [?cc :complete-cast/status ?cct2]
             [?n :name/name ?n.name]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

(def q-31a '[:find (min ?mi.info) (min ?mi-idx.info) (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)", "(written by)",
                            "(story)", "(story editor)"])]
             [?cn :company-name/name ?cn.name]
             [(like ?cn.name "Lionsgate%")]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "violence", "blood", "gore", "death",
                              "female-nudity", "hospital"])]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Horror", "Thriller"])]
             [?n :name/gender "m"]
             [?t :title/title ?t.title]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/company ?cn]
             [?n :name/name ?n.name]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

;; good plan
(def q-31b '[:find (min ?mi.info) (min ?mi-idx.info) (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)", "(written by)",
                            "(story)", "(story editor)"])]
             [?cn :company-name/name ?cn.name]
             [(like ?cn.name "Lionsgate%")]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "violence", "blood", "gore", "death",
                              "female-nudity", "hospital"])]
             [?mc :movie-companies/note ?mc.note]
             [(like ?mc.note "%(Blu-ray)%")]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Horror", "Thriller"])]
             [?n :name/gender "m"]
             [?t :title/production-year ?t.production-year]
             [(< 2000 ?t.production-year)]
             [?t :title/title ?t.title]
             [(or (like ?t.title "%Freddy%") (like ?t.title "%Jason%")
                  (like ?t.title "Saw%"))]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/company ?cn]
             [?n :name/name ?n.name]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

(def q-31c '[:find (min ?mi.info) (min ?mi-idx.info) (min ?n.name) (min ?t.title)
             :where
             [?ci :cast-info/note ?ci.note]
             [(in ?ci.note ["(writer)", "(head writer)", "(written by)",
                            "(story)", "(story editor)"])]
             [?cn :company-name/name ?cn.name]
             [(like ?cn.name "Lionsgate%")]
             [?it1 :info-type/info "genres"]
             [?it2 :info-type/info "votes"]
             [?k :keyword/keyword ?k.keyword]
             [(in ?k.keyword ["murder", "violence", "blood", "gore", "death",
                              "female-nudity", "hospital"])]
             [?mi :movie-info/info ?mi.info]
             [(in ?mi.info ["Horror", "Action", "Sci-Fi", "Thriller" "Crime",
                            "War"])]
             [?t :title/title ?t.title]
             [?mi :movie-info/movie ?t]
             [?mi-idx :movie-info-idx/movie ?t]
             [?ci :cast-info/movie ?t]
             [?mk :movie-keyword/movie ?t]
             [?mc :movie-companies/movie ?t]
             [?ci :cast-info/person ?n]
             [?mi :movie-info/info-type ?it1]
             [?mi-idx :movie-info-idx/info-type ?it2]
             [?mk :movie-keyword/keyword ?k]
             [?mc :movie-companies/company ?cn]
             [?n :name/name ?n.name]
             [?mi-idx :movie-info-idx/info ?mi-idx.info]
             ])

;; good plan
(def q-32a '[:find (min ?lt.link) (min ?t1.title) (min ?t2.title)
             :where
             [?k :keyword/keyword "10,000-mile-club"]
             [?mk :movie-keyword/keyword ?k]
             [?mk :movie-keyword/movie ?t1]
             [?ml :movie-link/movie ?t1]
             [?ml :movie-link/linked-movie ?t2]
             [?ml :movie-link/link-type ?lt]
             [?t1 :title/title ?t1.title]
             [?t2 :title/title ?t2.title]
             [?lt :link-type/link ?lt.link]
             ])

(def q-32b '[:find (min ?lt.link) (min ?t1.title) (min ?t2.title)
             :where
             [?k :keyword/keyword "character-name-in-title"]
             [?mk :movie-keyword/keyword ?k]
             [?mk :movie-keyword/movie ?t1]
             [?ml :movie-link/movie ?t1]
             [?ml :movie-link/linked-movie ?t2]
             [?ml :movie-link/link-type ?lt]
             [?t1 :title/title ?t1.title]
             [?t2 :title/title ?t2.title]
             [?lt :link-type/link ?lt.link]
             ])

(def q-33a '[:find (min ?cn1.name) (min ?cn2.name)
             (min ?mi-idx1.info) (min ?mi-idx2.info)
             (min ?t1.title) (min ?t2.title)
             :where
             [?cn1 :company-name/country-code "[us]"]
             [?it1 :info-type/info "rating"]
             [?it2 :info-type/info "rating"]
             [?kt1 :kind-type/kind ?kt1.kind]
             [(in ?kt1.kind ["tv series"])]
             [?kt2 :kind-type/kind ?kt2.kind]
             [(in ?kt2.kind ["tv series"])]
             [?lt :link-type/link ?lt.link]
             [(in ?lt.link ["sequel", "follows", "followed by"])]
             [?mi-idx2 :movie-info-idx/info-type ?it2]
             [?mi-idx2 :movie-info-idx/movie ?t2]
             [?mi-idx2 :movie-info-idx/info ?mi-idx2.info]
             [(< ?mi-idx2.info "3.0")]
             [?t2 :title/production-year ?t2.production-year]
             [(<= 2005 ?t2.production-year 2008)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t1]
             [?ml :movie-link/linked-movie ?t2]
             [?mi-idx1 :movie-info-idx/info-type ?it1]
             [?mi-idx1 :movie-info-idx/movie ?t1]
             [?mi-idx1 :movie-info-idx/info ?mi-idx1.info]
             [?t1 :title/kind ?kt1]
             [?mc1 :movie-companies/company ?cn1]
             [?mc1 :movie-companies/movie ?t1]
             [?t2 :title/kind ?kt2]
             [?mc2 :movie-companies/company ?cn2]
             [?mc2 :movie-companies/movie ?t2]
             [?t1 :title/title ?t1.title]
             [?t2 :title/title ?t2.title]
             [?cn1 :company-name/name ?cn1.name]
             [?cn2 :company-name/name ?cn2.name]
             ])

(def q-33b '[:find (min ?cn1.name) (min ?cn2.name)
             (min ?mi-idx1.info) (min ?mi-idx2.info)
             (min ?t1.title) (min ?t2.title)
             :where
             [?cn1 :company-name/country-code "[nl]"]
             [?it1 :info-type/info "rating"]
             [?it2 :info-type/info "rating"]
             [?kt1 :kind-type/kind ?kt1.kind]
             [(in ?kt1.kind ["tv series"])]
             [?kt2 :kind-type/kind ?kt2.kind]
             [(in ?kt2.kind ["tv series"])]
             [?lt :link-type/link ?lt.link]
             [(like ?lt.link "%follow%")]
             [?mi-idx2 :movie-info-idx/info-type ?it2]
             [?mi-idx2 :movie-info-idx/movie ?t2]
             [?mi-idx2 :movie-info-idx/info ?mi-idx2.info]
             [(< ?mi-idx2.info "3.0")]
             [?t2 :title/production-year 2007]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t1]
             [?ml :movie-link/linked-movie ?t2]
             [?mi-idx1 :movie-info-idx/info-type ?it1]
             [?mi-idx1 :movie-info-idx/movie ?t1]
             [?mi-idx1 :movie-info-idx/info ?mi-idx1.info]
             [?t1 :title/kind ?kt1]
             [?mc1 :movie-companies/company ?cn1]
             [?mc1 :movie-companies/movie ?t1]
             [?t2 :title/kind ?kt2]
             [?mc2 :movie-companies/company ?cn2]
             [?mc2 :movie-companies/movie ?t2]
             [?t1 :title/title ?t1.title]
             [?t2 :title/title ?t2.title]
             [?cn1 :company-name/name ?cn1.name]
             [?cn2 :company-name/name ?cn2.name]
             ])

;; good plan
(def q-33c '[:find (min ?cn1.name) (min ?cn2.name)
             (min ?mi-idx1.info) (min ?mi-idx2.info)
             (min ?t1.title) (min ?t2.title)
             :where
             [?cn1 :company-name/country-code ?cn1.country-code]
             [(not= ?cn1.country-code "[us]")]
             [?it1 :info-type/info "rating"]
             [?it2 :info-type/info "rating"]
             [?kt1 :kind-type/kind ?kt1.kind]
             [(in ?kt1.kind ["tv series" "episode"])]
             [?kt2 :kind-type/kind ?kt2.kind]
             [(in ?kt2.kind ["tv series" "episode"])]
             [?lt :link-type/link ?lt.link]
             [(in ?lt.link ["sequel", "follows", "followed by"])]
             [?mi-idx2 :movie-info-idx/info-type ?it2]
             [?mi-idx2 :movie-info-idx/movie ?t2]
             [?mi-idx2 :movie-info-idx/info ?mi-idx2.info]
             [(< ?mi-idx2.info "3.5")]
             [?t2 :title/production-year ?t2.production-year]
             [(<= 2000 ?t2.production-year 2010)]
             [?ml :movie-link/link-type ?lt]
             [?ml :movie-link/movie ?t1]
             [?ml :movie-link/linked-movie ?t2]
             [?mi-idx1 :movie-info-idx/info-type ?it1]
             [?mi-idx1 :movie-info-idx/movie ?t1]
             [?mi-idx1 :movie-info-idx/info ?mi-idx1.info]
             [?t1 :title/kind ?kt1]
             [?mc1 :movie-companies/company ?cn1]
             [?mc1 :movie-companies/movie ?t1]
             [?t2 :title/kind ?kt2]
             [?mc2 :movie-companies/company ?cn2]
             [?mc2 :movie-companies/movie ?t2]
             [?t1 :title/title ?t1.title]
             [?t2 :title/title ?t2.title]
             [?cn1 :company-name/name ?cn1.name]
             [?cn2 :company-name/name ?cn2.name]
             ])
