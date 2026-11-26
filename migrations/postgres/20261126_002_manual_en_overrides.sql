-- Exact source, country and field scoped corrections; no soil category fields.
CREATE TABLE IF NOT EXISTS manual_en_overrides (
    field TEXT NOT NULL CHECK (field IN (
        'contamination_other', 'observations', 'metals_info', 'image_description'
    )),
    country TEXT NOT NULL,
    original TEXT NOT NULL CHECK (btrim(original) <> ''),
    original_hash TEXT GENERATED ALWAYS AS (md5(original)) STORED,
    english TEXT NOT NULL CHECK (btrim(english) <> ''),
    updated_by TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (field, country, original_hash)
);

CREATE OR REPLACE FUNCTION manual_en_value(field_name TEXT, country_code TEXT, original_text TEXT)
RETURNS TEXT LANGUAGE sql VOLATILE AS $$
    SELECT english FROM manual_en_overrides
    WHERE field = field_name
      AND country = upper(btrim(coalesce(country_code, '')))
      AND original_hash = md5(original_text)
      AND original = original_text
$$;

CREATE OR REPLACE FUNCTION preserve_sample_manual_en() RETURNS trigger LANGUAGE plpgsql AS $
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (OLD.contamination_other_orig, OLD.country_code)
                IS DISTINCT FROM (NEW.contamination_other_orig, NEW.country_code)
            AND NEW.contamination_other_en IS NOT DISTINCT FROM OLD.contamination_other_en
            AND OLD.contamination_other_en = manual_en_value('contamination_other', OLD.country_code, OLD.contamination_other_orig)
        THEN
            NEW.contamination_other_en := NULL;
        END IF;
        IF (OLD.observations_orig, OLD.country_code)
                IS DISTINCT FROM (NEW.observations_orig, NEW.country_code)
            AND NEW.observations_en IS NOT DISTINCT FROM OLD.observations_en
            AND OLD.observations_en = manual_en_value('observations', OLD.country_code, OLD.observations_orig)
        THEN
            NEW.observations_en := NULL;
        END IF;
        IF (OLD.metals_info_orig, OLD.country_code)
                IS DISTINCT FROM (NEW.metals_info_orig, NEW.country_code)
            AND NEW.metals_info_en IS NOT DISTINCT FROM OLD.metals_info_en
            AND OLD.metals_info_en = manual_en_value('metals_info', OLD.country_code, OLD.metals_info_orig)
        THEN
            NEW.metals_info_en := NULL;
        END IF;
    END IF;
    NEW.contamination_other_en := coalesce(
        manual_en_value('contamination_other', NEW.country_code, NEW.contamination_other_orig),
        NEW.contamination_other_en);
    NEW.observations_en := coalesce(
        manual_en_value('observations', NEW.country_code, NEW.observations_orig),
        NEW.observations_en);
    NEW.metals_info_en := coalesce(
        manual_en_value('metals_info', NEW.country_code, NEW.metals_info_orig),
        NEW.metals_info_en);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION preserve_image_manual_en() RETURNS trigger LANGUAGE plpgsql AS $
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (OLD.image_description_orig, OLD.country_code)
                IS DISTINCT FROM (NEW.image_description_orig, NEW.country_code)
            AND NEW.image_description_en IS NOT DISTINCT FROM OLD.image_description_en
            AND OLD.image_description_en = manual_en_value('image_description', OLD.country_code, OLD.image_description_orig)
        THEN
            NEW.image_description_en := NULL;
        END IF;
    END IF;
    NEW.image_description_en := coalesce(
        manual_en_value('image_description', NEW.country_code, NEW.image_description_orig),
        NEW.image_description_en);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS preserve_sample_manual_en ON samples;
CREATE TRIGGER preserve_sample_manual_en
BEFORE INSERT OR UPDATE OF country_code, contamination_other_orig, contamination_other_en,
    observations_orig, observations_en, metals_info_orig, metals_info_en ON samples
FOR EACH ROW EXECUTE FUNCTION preserve_sample_manual_en();

DROP TRIGGER IF EXISTS preserve_image_manual_en ON sample_images;
CREATE TRIGGER preserve_image_manual_en
BEFORE INSERT OR UPDATE OF country_code, image_description_orig, image_description_en ON sample_images
FOR EACH ROW EXECUTE FUNCTION preserve_image_manual_en();
