CREATE TABLE public.experiment_snapshots (
    id SERIAL,
	experiment_id integer NOT NULL,
    content jsonb NOT NULL,
	created_at timestamp with time zone NOT NULL DEFAULT NOW(),

	CONSTRAINT fk_experiments FOREIGN KEY(experiment_id) REFERENCES public.experiments(id)
);

CREATE TABLE public.trial_snapshots (
	id SERIAL,
	request_id bytea NOT NULL,
	trial_id integer NOT NULL,
	content jsonb NOT NULL,
	created_at timestamp with time zone NOT NULL DEFAULT NOW(),

	CONSTRAINT fk_trials FOREIGN KEY(trial_id) REFERENCES public.trials(id)
);

ALTER TABLE public.trials ADD COLUMN request_id text NULL;