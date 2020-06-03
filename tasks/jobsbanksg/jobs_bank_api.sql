CREATE SCHEMA jobsbanksg;

DROP TABLE IF EXISTS jobsbanksg.tbl_jobs_api;

CREATE TABLE jobsbanksg.tbl_jobs_api (
    job_uuid            text,
    source_code         text,
    job_title           text,
    company_name        text,
    company_uen         text,
    job_posting_date    text,
    job_closing_date    text,
    experience_min      text,
    salary_min          numeric,
    salary_max          numeric,
    salary_type         text,
    job_description     text,
    address_block       text,
    address_street      text,
    address_floor       text,
    address_unit        text,
    address_building    text,
    address_postal_code text,
    blob                text
);