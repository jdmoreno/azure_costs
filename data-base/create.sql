--
-- PostgreSQL database dump
--

-- Dumped from database version 16.2 (Debian 16.2-1.pgdg120+2)
-- Dumped by pg_dump version 16.0

-- Started on 2024-03-12 09:57:34

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 5 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: pg_database_owner
--

CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO pg_database_owner;

--
-- TOC entry 3396 (class 0 OID 0)
-- Dependencies: 5
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: pg_database_owner
--

COMMENT ON SCHEMA public IS 'standard public schema';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 218 (class 1259 OID 16554)
-- Name: cost_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.cost_data (
    file_name character varying NOT NULL,
    cost_period date NOT NULL,
    subscription_name character varying NOT NULL,
    subscription_id character varying NOT NULL,
    resource_group character varying NOT NULL,
    resource_group_id character varying NOT NULL,
    resource character varying NOT NULL,
    resource_id character varying NOT NULL,
    resource_type character varying NOT NULL,
    resource_location character varying NOT NULL,
    tags character varying NOT NULL,
    cost_c real,
    cost_currency character varying NOT NULL,
    cost_usd real,
    area character varying NOT NULL,
    capability character varying NOT NULL,
    environment character varying NOT NULL,
    create_datetime timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE public.cost_data OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 16389)
-- Name: reference_data; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.reference_data (
    reference_c character varying NOT NULL,
    key_c character varying NOT NULL,
    value_c character varying
);


ALTER TABLE public.reference_data OWNER TO postgres;

--
-- TOC entry 224 (class 1259 OID 25232)
-- Name: resource_groups; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.resource_groups (
    subscription_name character varying NOT NULL,
    resource_group character varying DEFAULT ''::character varying,
    affix character varying
);


ALTER TABLE public.resource_groups OWNER TO postgres;

--
-- TOC entry 3246 (class 2606 OID 16560)
-- Name: cost_data pk_cost_data; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.cost_data
    ADD CONSTRAINT pk_cost_data PRIMARY KEY (cost_period, subscription_id, resource_group_id, resource_id);


--
-- TOC entry 3244 (class 2606 OID 16395)
-- Name: reference_data pk_reference_data; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.reference_data
    ADD CONSTRAINT pk_reference_data PRIMARY KEY (reference_c, key_c);


--
-- TOC entry 3247 (class 1259 OID 25238)
-- Name: IX_resource_groups; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX "IX_resource_groups" ON public.resource_groups USING btree (subscription_name, resource_group) WITH (deduplicate_items='true');


-- Completed on 2024-03-12 09:57:34

--
-- PostgreSQL database dump complete
--

