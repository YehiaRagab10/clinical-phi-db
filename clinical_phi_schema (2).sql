-- =============================================================================
-- Synthetic Belgian-Dutch (Flemish) clinical PHI/PII dataset
-- Relation-preserving PostgreSQL schema  (PostgreSQL 12+; identity cols need 10+)
-- v4 (final) -- pedigree family history, personal history with dates/laterality,
--       multi-role report layer, hospital sites, dated investigations,
--       + sample_log dedup ledger (durable "no combination ever repeats").
-- -----------------------------------------------------------------------------
-- LOADING FOUND DATA (easy path):
--   You only bulk-load DICTIONARY tables from online sources. Everything about a
--   patient's history/family is *generated* into the INSTANCE tables, so you never
--   have to hand-curate histories.
--     drugs+doses  -> drug, drug_dose            (e.g. CBIP/BCFI export)
--     conditions   -> condition (+ is_heritable, typical_onset_age help generation)
--     tests/procs  -> test, procedure            (RIZIV nomenclature / LOINC)
--     names        -> name_origin, given_name, surname   (Statbel)
--     geography    -> country..address           (BeST Address)
--     relationships-> relationship (tiny fixed list, load once)
--
-- Layers:  DICTIONARY (reference data) + INSTANCE/FACT (generated) + LINK (knowledge).
-- =============================================================================

DROP SCHEMA IF EXISTS phi CASCADE;
CREATE SCHEMA phi;
SET search_path TO phi, public;

-- =============================================================================
-- SECTION 1 -- GEOGRAPHY  (country -> province -> city -> postcode -> street -> address)
-- =============================================================================

CREATE TABLE country (
    country_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text NOT NULL,
    iso2       char(2),
    CONSTRAINT uq_country_name UNIQUE (name)
);

CREATE TABLE province (
    province_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text NOT NULL,
    country_id  bigint NOT NULL REFERENCES country (country_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    nis_code    text,
    CONSTRAINT uq_province UNIQUE (name, country_id)
);

CREATE TABLE city (
    city_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text NOT NULL,
    name_nl     text,
    name_fr     text,
    country_id  bigint NOT NULL REFERENCES country (country_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    province_id bigint REFERENCES province (province_id) ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT uq_city UNIQUE (name, country_id, province_id)
);

CREATE TABLE postcode (
    postcode_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code        text NOT NULL,
    city_id     bigint NOT NULL REFERENCES city (city_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT ck_postcode_code CHECK (code ~ '^[A-Za-z0-9][A-Za-z0-9 -]{1,11}$'),
    CONSTRAINT uq_postcode UNIQUE (code, city_id)
);

CREATE TABLE street (
    street_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        text NOT NULL,
    name_nl     text,
    name_fr     text,
    postcode_id bigint NOT NULL REFERENCES postcode (postcode_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT uq_street UNIQUE (name, postcode_id)
);

CREATE TABLE address (
    address_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    street_id    bigint NOT NULL REFERENCES street (street_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    house_number text NOT NULL,
    box_number   text,
    CONSTRAINT uq_address UNIQUE (street_id, house_number, box_number)
);

-- =============================================================================
-- SECTION 2 -- NAMES BY ORIGIN + PROFESSION + RELATIONSHIP
-- =============================================================================

CREATE TABLE name_origin (
    origin_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    label     text NOT NULL,
    CONSTRAINT uq_name_origin UNIQUE (label)
);

CREATE TABLE given_name (
    given_name_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          text NOT NULL,
    sex           char(1) NOT NULL DEFAULT 'X'
        CONSTRAINT ck_given_sex CHECK (sex IN ('M','F','X')),
    origin_id     bigint NOT NULL REFERENCES name_origin (origin_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    frequency     integer CONSTRAINT ck_given_freq CHECK (frequency IS NULL OR frequency >= 0),
    CONSTRAINT uq_given_name UNIQUE (name, sex, origin_id)
);

CREATE TABLE surname (
    surname_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          text NOT NULL,
    origin_id     bigint NOT NULL REFERENCES name_origin (origin_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    tussenvoegsel text,
    frequency     integer CONSTRAINT ck_surname_freq CHECK (frequency IS NULL OR frequency >= 0),
    CONSTRAINT uq_surname UNIQUE (name, origin_id)
);

CREATE TABLE profession (
    profession_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name_nl       text NOT NULL,
    CONSTRAINT uq_profession UNIQUE (name_nl)
);

-- Reusable family-relationship descriptors (load a small fixed list once).
CREATE TABLE relationship (
    relationship_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code            text NOT NULL,              -- mother, father, paternal_grandfather, niece...
    label_nl        text,                       -- moeder, vader, grootvader (paternele zijde)...
    label_en        text,
    degree          integer,                    -- 1=parent/child/sib, 2=grandparent..., NULL=other
    default_lineage text CONSTRAINT ck_rel_lineage
        CHECK (default_lineage IS NULL OR default_lineage IN ('paternal','maternal')),
    CONSTRAINT uq_relationship UNIQUE (code)
);

-- =============================================================================
-- SECTION 3 -- ORGANIZATION  (hospital + sites, specialty, department, provider)
-- =============================================================================

CREATE TABLE medical_specialty (
    specialty_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         text NOT NULL,
    riziv_qualification_code text,
    CONSTRAINT uq_specialty UNIQUE (name)
);

CREATE TABLE hospital (
    hospital_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name     text NOT NULL,
    abbreviation  text,
    hospital_type text,                         -- UZ / AZ / ZNA / GZA / ZOL / other
    campus        text,
    url           text,                         -- e.g. https://www.azorg.example.test
    address_id    bigint REFERENCES address (address_id) ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT uq_hospital UNIQUE (full_name, campus)
);

-- A hospital can have many sites, each with its own address + phone (letter footer).
CREATE TABLE hospital_site (
    site_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hospital_id bigint NOT NULL REFERENCES hospital (hospital_id) ON UPDATE CASCADE ON DELETE CASCADE,
    name        text,                           -- optional site/campus label
    address_id  bigint REFERENCES address (address_id) ON UPDATE CASCADE ON DELETE SET NULL,
    phone       text,
    CONSTRAINT uq_hospital_site UNIQUE (hospital_id, address_id)
);

CREATE TABLE clinic_department (
    department_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          text NOT NULL,
    hospital_id   bigint NOT NULL REFERENCES hospital (hospital_id) ON UPDATE CASCADE ON DELETE CASCADE,
    specialty_id  bigint REFERENCES medical_specialty (specialty_id) ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT uq_department UNIQUE (name, hospital_id)
);

CREATE TABLE provider (                         -- doctor / responsible / validator
    provider_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    given_name_id bigint REFERENCES given_name (given_name_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    surname_id    bigint REFERENCES surname (surname_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    riziv_number  char(11),
    specialty_id  bigint REFERENCES medical_specialty (specialty_id) ON UPDATE CASCADE ON DELETE SET NULL,
    department_id bigint REFERENCES clinic_department (department_id) ON UPDATE CASCADE ON DELETE SET NULL,
    CONSTRAINT ck_provider_riziv CHECK (riziv_number IS NULL OR riziv_number ~ '^[0-9]{11}$')
);

-- =============================================================================
-- SECTION 4 -- CLINICAL DICTIONARIES
-- =============================================================================

CREATE TABLE blood_type (
    blood_type_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    abo           text    NOT NULL CONSTRAINT ck_abo    CHECK (abo IN ('A','B','AB','O')),
    rhesus        char(1) NOT NULL CONSTRAINT ck_rhesus CHECK (rhesus IN ('+','-')),
    CONSTRAINT uq_blood_type UNIQUE (abo, rhesus)
);

CREATE TABLE symptom (
    symptom_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text NOT NULL,
    snomed_id  text,
    CONSTRAINT uq_symptom UNIQUE (name)
);

CREATE TABLE test (
    test_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text NOT NULL,
    loinc_code text,
    unit       text,
    ref_low    numeric,
    ref_high   numeric,
    CONSTRAINT uq_test UNIQUE (name)
);

CREATE TABLE procedure (
    procedure_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         text NOT NULL,
    riziv_nomen_code text,
    snomed_id    text,
    CONSTRAINT uq_procedure UNIQUE (name)
);

CREATE TABLE condition (
    condition_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name              text NOT NULL,
    icd10_code        text,
    snomed_id         text,
    is_heritable      boolean DEFAULT false,    -- helps the generator cluster family disease
    typical_onset_age integer                   -- plausible onset age for family-history rendering
        CONSTRAINT ck_cond_onset CHECK (typical_onset_age IS NULL OR typical_onset_age >= 0),
    CONSTRAINT uq_condition_icd10 UNIQUE (icd10_code)
);

CREATE TABLE drug (
    drug_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    brand_name text,
    inn_name   text NOT NULL,
    atc_code   text,
    cnk_code   text,
    CONSTRAINT uq_drug_cnk UNIQUE (cnk_code)
);

CREATE TABLE drug_dose (
    dose_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    drug_id      bigint NOT NULL REFERENCES drug (drug_id) ON UPDATE CASCADE ON DELETE CASCADE,
    amount       numeric,
    unit         text,
    frequency    text,
    route        text,
    dose_display text,
    CONSTRAINT uq_dose UNIQUE (drug_id, amount, unit, frequency, route)
);

CREATE TABLE allergy (
    allergy_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    substance_name text NOT NULL,
    atc_code       text,
    snomed_id      text,
    CONSTRAINT uq_allergy UNIQUE (substance_name)
);

CREATE TABLE treatment (
    treatment_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         text NOT NULL,
    CONSTRAINT uq_treatment UNIQUE (name)
);

-- =============================================================================
-- SECTION 5 -- KNOWLEDGE LINK TABLES (allowed combinations)
-- =============================================================================

CREATE TABLE condition_symptom (
    condition_id bigint NOT NULL REFERENCES condition (condition_id) ON UPDATE CASCADE ON DELETE CASCADE,
    symptom_id   bigint NOT NULL REFERENCES symptom   (symptom_id)   ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (condition_id, symptom_id)
);

CREATE TABLE condition_test (
    condition_id bigint NOT NULL REFERENCES condition (condition_id) ON UPDATE CASCADE ON DELETE CASCADE,
    test_id      bigint NOT NULL REFERENCES test      (test_id)      ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (condition_id, test_id)
);

CREATE TABLE condition_drug (
    condition_id bigint NOT NULL REFERENCES condition (condition_id) ON UPDATE CASCADE ON DELETE CASCADE,
    drug_id      bigint NOT NULL REFERENCES drug      (drug_id)      ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (condition_id, drug_id)
);

CREATE TABLE condition_treatment (
    condition_id bigint NOT NULL REFERENCES condition (condition_id) ON UPDATE CASCADE ON DELETE CASCADE,
    treatment_id bigint NOT NULL REFERENCES treatment (treatment_id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (condition_id, treatment_id)
);

CREATE TABLE procedure_condition (
    procedure_id bigint NOT NULL REFERENCES procedure (procedure_id) ON UPDATE CASCADE ON DELETE CASCADE,
    condition_id bigint NOT NULL REFERENCES condition (condition_id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (procedure_id, condition_id)
);

CREATE TABLE allergy_drug (
    allergy_id     bigint  NOT NULL REFERENCES allergy (allergy_id) ON UPDATE CASCADE ON DELETE CASCADE,
    drug_id        bigint  NOT NULL REFERENCES drug    (drug_id)    ON UPDATE CASCADE ON DELETE CASCADE,
    cross_reactive boolean NOT NULL DEFAULT false,
    PRIMARY KEY (allergy_id, drug_id)
);

-- =============================================================================
-- SECTION 6 -- INSTANCE / FACT TABLES
-- =============================================================================

CREATE TABLE patient (
    patient_id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    given_name_id bigint REFERENCES given_name (given_name_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    surname_id    bigint REFERENCES surname (surname_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    sex           char(1) NOT NULL CONSTRAINT ck_patient_sex CHECK (sex IN ('M','F','X')),
    birth_date    date,
    insz          char(11),
    address_id    bigint REFERENCES address (address_id) ON UPDATE CASCADE ON DELETE SET NULL,
    profession_id bigint REFERENCES profession (profession_id) ON UPDATE CASCADE ON DELETE SET NULL,
    mrn           text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ck_patient_insz CHECK (insz IS NULL OR insz ~ '^[0-9]{11}$')
);

CREATE TABLE patient_contact (
    contact_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_id   bigint NOT NULL REFERENCES patient (patient_id) ON UPDATE CASCADE ON DELETE CASCADE,
    kind         text NOT NULL CONSTRAINT ck_contact_kind
                 CHECK (kind IN ('phone_mobile','phone_landline','fax','email','url')),
    value        text NOT NULL,
    format_label text
);

-- ---- PERSONAL medical history (Voorgeschiedenis): patient's OWN past conditions,
--      each with a date and (optional) laterality/detail. -----------------------
CREATE TABLE patient_condition_history (
    pch_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_id     bigint NOT NULL REFERENCES patient   (patient_id)   ON UPDATE CASCADE ON DELETE CASCADE,
    condition_id   bigint NOT NULL REFERENCES condition (condition_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    onset_date     date,                        -- 2024-01-15 ; for "12-2025" use 2025-12-01
    onset_precision text CONSTRAINT ck_pch_prec CHECK (onset_precision IS NULL OR onset_precision IN ('day','month','year')),
    laterality     text CONSTRAINT ck_pch_lat  CHECK (laterality IS NULL OR laterality IN ('left','right','bilateral')),
    detail         text,                        -- free detail ("na val", etc.)
    note           text
);

-- ---- FAMILY history as a PEDIGREE: relatives are people (optionally linked to a
--      real patient in the pool), and relatives carry their own conditions. -----
CREATE TABLE relative (
    relative_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_id         bigint NOT NULL REFERENCES patient (patient_id) ON UPDATE CASCADE ON DELETE CASCADE, -- the proband
    relationship_id    bigint REFERENCES relationship (relationship_id) ON UPDATE CASCADE ON DELETE SET NULL,
    relationship_detail text,                   -- precise phrasing: "dochter van broer van vader"
    lineage            text CONSTRAINT ck_rel_line CHECK (lineage IS NULL OR lineage IN ('paternal','maternal')),
    sex                char(1) CONSTRAINT ck_rel_sex CHECK (sex IS NULL OR sex IN ('M','F','X')),
    linked_patient_id  bigint REFERENCES patient (patient_id) ON UPDATE CASCADE ON DELETE SET NULL,  -- "grouping": inherit this person's history
    CONSTRAINT ck_relative_not_self CHECK (linked_patient_id IS NULL OR linked_patient_id <> patient_id)
);

CREATE TABLE relative_condition (
    rc_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    relative_id  bigint NOT NULL REFERENCES relative  (relative_id)  ON UPDATE CASCADE ON DELETE CASCADE,
    condition_id bigint NOT NULL REFERENCES condition (condition_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    onset_age    integer CONSTRAINT ck_rc_age CHECK (onset_age IS NULL OR onset_age >= 0),  -- "op 50-jarige leeftijd"
    note         text,
    CONSTRAINT uq_relative_condition UNIQUE (relative_id, condition_id)
);

CREATE TABLE visit (
    visit_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_id     bigint NOT NULL REFERENCES patient (patient_id) ON UPDATE CASCADE ON DELETE CASCADE,
    provider_id    bigint REFERENCES provider (provider_id) ON UPDATE CASCADE ON DELETE SET NULL,
    department_id  bigint REFERENCES clinic_department (department_id) ON UPDATE CASCADE ON DELETE SET NULL,
    blood_type_id  bigint REFERENCES blood_type (blood_type_id) ON UPDATE CASCADE ON DELETE SET NULL,
    visit_type     text,
    visit_datetime timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- A clinical letter/report about a visit, with several providers in distinct roles.
CREATE TABLE report (
    report_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_id     bigint NOT NULL REFERENCES patient (patient_id) ON UPDATE CASCADE ON DELETE CASCADE,
    visit_id       bigint REFERENCES visit (visit_id) ON UPDATE CASCADE ON DELETE SET NULL,
    department_id  bigint REFERENCES clinic_department (department_id) ON UPDATE CASCADE ON DELETE SET NULL,
    report_type    text,                        -- 'brief' / 'ontslagbrief' / ...
    report_datetime timestamptz,                -- DATUM
    validated_on   timestamptz,                 -- Validatie
    body_text      text                         -- optional rendered narrative
);

CREATE TABLE report_provider (
    report_id   bigint NOT NULL REFERENCES report   (report_id)   ON UPDATE CASCADE ON DELETE CASCADE,
    provider_id bigint NOT NULL REFERENCES provider (provider_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    role        text   NOT NULL CONSTRAINT ck_report_role
                CHECK (role IN ('author','sender','responsible','validator','cosigner','cc')),
    PRIMARY KEY (report_id, provider_id, role)
);

CREATE TABLE observation (
    observation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    visit_id       bigint NOT NULL REFERENCES visit (visit_id) ON UPDATE CASCADE ON DELETE CASCADE,
    test_id        bigint REFERENCES test (test_id) ON UPDATE CASCADE ON DELETE SET NULL,
    procedure_id   bigint REFERENCES procedure (procedure_id) ON UPDATE CASCADE ON DELETE SET NULL,
    observed_on    date,                        -- date of the finding/investigation
    category       text,                        -- 'conclusion' (BESLUIT) / 'pathology' (APO) / 'exam' ...
    result_text    text NOT NULL
);

CREATE TABLE measurement (
    measurement_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    visit_id       bigint NOT NULL REFERENCES visit (visit_id) ON UPDATE CASCADE ON DELETE CASCADE,
    test_id        bigint REFERENCES test (test_id) ON UPDATE CASCADE ON DELETE SET NULL,
    label          text,
    value          numeric,
    value_text     text,
    unit           text,
    CONSTRAINT ck_measurement_value CHECK (value IS NOT NULL OR value_text IS NOT NULL)
);

-- =============================================================================
-- SECTION 6b -- GENERATION BOOKKEEPING
-- Durable dedup: the sampler reserves each record's canonical signature here.
-- The UNIQUE(signature) makes "no combination is ever repeated" hold even across
-- separate sampler runs / machines, not just within one process.
-- =============================================================================

CREATE TABLE sample_log (
    sample_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    signature  text NOT NULL,                   -- hash of the chosen entity combination
    split      text,                            -- 'train' / 'val' / 'test'
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_sample_signature UNIQUE (signature)
);

-- =============================================================================
-- SECTION 7 -- INSTANCE LINK TABLES (populate only via the knowledge links)
-- =============================================================================

CREATE TABLE visit_symptom (
    visit_id   bigint NOT NULL REFERENCES visit   (visit_id)   ON UPDATE CASCADE ON DELETE CASCADE,
    symptom_id bigint NOT NULL REFERENCES symptom (symptom_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    PRIMARY KEY (visit_id, symptom_id)
);

CREATE TABLE visit_test (
    visit_id bigint NOT NULL REFERENCES visit (visit_id) ON UPDATE CASCADE ON DELETE CASCADE,
    test_id  bigint NOT NULL REFERENCES test  (test_id)  ON UPDATE CASCADE ON DELETE RESTRICT,
    PRIMARY KEY (visit_id, test_id)
);

CREATE TABLE visit_procedure (
    visit_id     bigint NOT NULL REFERENCES visit     (visit_id)     ON UPDATE CASCADE ON DELETE CASCADE,
    procedure_id bigint NOT NULL REFERENCES procedure (procedure_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    performed_on date,                          -- "Totale coloscopie 24/04"
    PRIMARY KEY (visit_id, procedure_id)
);

CREATE TABLE visit_condition (
    visit_id     bigint NOT NULL REFERENCES visit     (visit_id)     ON UPDATE CASCADE ON DELETE CASCADE,
    condition_id bigint NOT NULL REFERENCES condition (condition_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    PRIMARY KEY (visit_id, condition_id)
);

CREATE TABLE visit_drug (
    visit_id bigint NOT NULL REFERENCES visit (visit_id) ON UPDATE CASCADE ON DELETE CASCADE,
    drug_id  bigint NOT NULL REFERENCES drug  (drug_id)  ON UPDATE CASCADE ON DELETE RESTRICT,
    dose_id  bigint          REFERENCES drug_dose (dose_id) ON UPDATE CASCADE ON DELETE SET NULL,
    PRIMARY KEY (visit_id, drug_id)
);

CREATE TABLE visit_treatment (
    visit_id     bigint NOT NULL REFERENCES visit     (visit_id)     ON UPDATE CASCADE ON DELETE CASCADE,
    treatment_id bigint NOT NULL REFERENCES treatment (treatment_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    PRIMARY KEY (visit_id, treatment_id)
);

CREATE TABLE patient_allergy (
    patient_id bigint NOT NULL REFERENCES patient (patient_id) ON UPDATE CASCADE ON DELETE CASCADE,
    allergy_id bigint NOT NULL REFERENCES allergy (allergy_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    PRIMARY KEY (patient_id, allergy_id)
);

-- =============================================================================
-- SECTION 8 -- INDEXES ON FOREIGN KEYS
-- =============================================================================

CREATE INDEX ix_province_country        ON province          (country_id);
CREATE INDEX ix_city_country            ON city              (country_id);
CREATE INDEX ix_city_province           ON city              (province_id);
CREATE INDEX ix_postcode_city           ON postcode          (city_id);
CREATE INDEX ix_street_postcode         ON street            (postcode_id);
CREATE INDEX ix_address_street          ON address           (street_id);
CREATE INDEX ix_given_name_origin       ON given_name        (origin_id);
CREATE INDEX ix_surname_origin          ON surname           (origin_id);
CREATE INDEX ix_hospital_address        ON hospital          (address_id);
CREATE INDEX ix_hospital_site_hospital  ON hospital_site     (hospital_id);
CREATE INDEX ix_hospital_site_address   ON hospital_site     (address_id);
CREATE INDEX ix_department_hospital     ON clinic_department (hospital_id);
CREATE INDEX ix_department_specialty    ON clinic_department (specialty_id);
CREATE INDEX ix_provider_given          ON provider          (given_name_id);
CREATE INDEX ix_provider_surname        ON provider          (surname_id);
CREATE INDEX ix_provider_specialty      ON provider          (specialty_id);
CREATE INDEX ix_provider_department     ON provider          (department_id);
CREATE INDEX ix_drug_dose_drug          ON drug_dose         (drug_id);
CREATE INDEX ix_patient_given           ON patient           (given_name_id);
CREATE INDEX ix_patient_surname         ON patient           (surname_id);
CREATE INDEX ix_patient_address         ON patient           (address_id);
CREATE INDEX ix_patient_profession      ON patient           (profession_id);
CREATE INDEX ix_patient_contact_patient ON patient_contact   (patient_id);
CREATE INDEX ix_pch_patient             ON patient_condition_history (patient_id);
CREATE INDEX ix_pch_condition           ON patient_condition_history (condition_id);
CREATE INDEX ix_relative_patient        ON relative          (patient_id);
CREATE INDEX ix_relative_relationship   ON relative          (relationship_id);
CREATE INDEX ix_relative_linked         ON relative          (linked_patient_id);
CREATE INDEX ix_relative_condition_rel  ON relative_condition (relative_id);
CREATE INDEX ix_relative_condition_cond ON relative_condition (condition_id);
CREATE INDEX ix_visit_patient           ON visit             (patient_id);
CREATE INDEX ix_visit_provider          ON visit             (provider_id);
CREATE INDEX ix_visit_department        ON visit             (department_id);
CREATE INDEX ix_visit_blood_type        ON visit             (blood_type_id);
CREATE INDEX ix_report_patient          ON report            (patient_id);
CREATE INDEX ix_report_visit            ON report            (visit_id);
CREATE INDEX ix_report_department       ON report            (department_id);
CREATE INDEX ix_report_provider_prov    ON report_provider   (provider_id);
CREATE INDEX ix_observation_visit       ON observation       (visit_id);
CREATE INDEX ix_observation_test        ON observation       (test_id);
CREATE INDEX ix_observation_procedure   ON observation       (procedure_id);
CREATE INDEX ix_measurement_visit       ON measurement       (visit_id);
CREATE INDEX ix_measurement_test        ON measurement       (test_id);
CREATE INDEX ix_condition_symptom_sym   ON condition_symptom (symptom_id);
CREATE INDEX ix_condition_test_test     ON condition_test    (test_id);
CREATE INDEX ix_condition_drug_drug     ON condition_drug    (drug_id);
CREATE INDEX ix_condition_treatment_trt ON condition_treatment (treatment_id);
CREATE INDEX ix_procedure_condition_cond ON procedure_condition (condition_id);
CREATE INDEX ix_allergy_drug_drug       ON allergy_drug      (drug_id);
CREATE INDEX ix_visit_symptom_sym       ON visit_symptom     (symptom_id);
CREATE INDEX ix_visit_test_test         ON visit_test        (test_id);
CREATE INDEX ix_visit_procedure_proc    ON visit_procedure   (procedure_id);
CREATE INDEX ix_visit_condition_cond    ON visit_condition   (condition_id);
CREATE INDEX ix_visit_drug_drug         ON visit_drug        (drug_id);
CREATE INDEX ix_visit_drug_dose         ON visit_drug        (dose_id);
CREATE INDEX ix_visit_treatment_trt     ON visit_treatment   (treatment_id);
CREATE INDEX ix_patient_allergy_allergy ON patient_allergy   (allergy_id);
CREATE INDEX ix_sample_log_split        ON sample_log        (split);

-- =============================================================================
-- SECTION 9 -- COMMENTS
-- =============================================================================
COMMENT ON SCHEMA phi IS 'Synthetic Belgian-Dutch clinical PHI/PII dataset (generation track).';
COMMENT ON TABLE relative       IS 'Pedigree node: a family member of the proband. linked_patient_id groups a real person in from the pool so their conditions become this family history.';
COMMENT ON TABLE relative_condition IS 'A relative''s disease with onset age -> "darmkanker op 50-jarige leeftijd".';
COMMENT ON TABLE patient_condition_history IS 'Voorgeschiedenis: the patient''s OWN past conditions with date + laterality.';
COMMENT ON TABLE report_provider IS 'Provider roles on a letter: author/sender (Verzonden door), responsible (Verantwoordelijke), validator (gevalideerd door), cosigner.';
COMMENT ON TABLE hospital_site  IS 'Per-site address+phone for the letter footer; one hospital, many sites.';
COMMENT ON COLUMN condition.is_heritable IS 'Set true for family-clustering diseases (e.g. colorectal cancer) so the generator can seed pedigrees.';
