CREATE TABLE users (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       name STRING NOT NULL,
       username STRING NOT NULL,
       hashed_password STRING NOT NULL,
       is_admin BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE score_sheet_templates (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       name STRING NOT NULL,
       type STRING NOT NULL CHECK (type IN ('Interview', 'Performance')),
       timings STRING[] NOT NULL DEFAULT ARRAY[]
);

CREATE TABLE score_sheet_template_sections (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       title STRING NOT NULL,
       score_sheet_template UUID NOT NULL REFERENCES score_sheet_templates (id),
       description STRING NOT NULL,
       max_value INT NOT NULL DEFAULT 0,
       multiplier INT NOT NULL DEFAULT 1,
       display_order INT NOT NULL DEFAULT 0,
       INDEX (score_sheet_template)
);

CREATE TABLE divisions (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       name STRING NOT NULL,
       league STRING NOT NULL CHECK (league IN ('Soccer', 'Rescue', 'On Stage')),
       competition_rounds INT NOT NULL DEFAULT 0,
       final_rounds INT NOT NULL DEFAULT 0,
       interview_template UUID REFERENCES score_sheet_templates (id),
       performance_template UUID REFERENCES score_sheet_templates (id),
       INDEX (interview_template),
       INDEX (performance_template)
);

CREATE TABLE institutions(
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       name STRING NOT NULL
);

CREATE TABLE teams (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       name STRING NOT NULL,
       institution UUID NOT NULL REFERENCES institutions (id),
       division UUID NOT NULL REFERENCES divisions (id),
       import_id STRING,
       INDEX (institution),
       INDEX (division)
);

CREATE TABLE team_members (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       name STRING NOT NULL,
       gender STRING DEFAULT 'Not Specified' NOT NULL CHECK (gender IN ('Male', 'Female', 'Not Specified')),
       team UUID NOT NULL REFERENCES teams (id),
       INDEX (team)
);


CREATE TABLE score_sheets (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       division UUID NOT NULL REFERENCES divisions (id),
       team UUID NOT NULL REFERENCES teams (id),
       template UUID NOT NULL REFERENCES score_sheet_templates (id),
       timings JSONB NOT NULL DEFAULT json_build_array(),
       author UUID NOT NULL REFERENCES users (id),
       comments STRING NOT NULL,
       round INT NOT NULL DEFAULT 0,
       created_at TIMESTAMP NOT NULL DEFAULT current_timestamp(),
       INDEX (division),
       INDEX (template),
       INDEX (team),
       INDEX (author)
);

CREATE TABLE score_sheet_sections (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       section UUID NOT NULL REFERENCES score_sheet_template_sections (id),
       value DECIMAL(10,5) NOT NULL DEFAULT 0.0,
       score_sheet UUID NOT NULL REFERENCES score_sheets (id),
       INDEX (section)
);

CREATE TABLE team_checkins (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       team UUID NOT NULL REFERENCES teams (id),
       agent UUID NOT NULL REFERENCES users (id),
       comments string NOT NULL,
       in_time TIMESTAMP NOT NULL DEFAULT current_timestamp(),
       INDEX (team),
       INDEX (agent)
);

CREATE TABLE sheet_token (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       token string NOT NULL,
       token_type string NOT NULL DEFAULT 'auth'
);
