DROP TABLE public.experiment_snapshots;

DROP TABLE public.trial_snapshots;

ALTER TABLE public.trials DROP COLUMN request_id;
